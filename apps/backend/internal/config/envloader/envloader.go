package envloader

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/joho/godotenv"
)

var (
	loadOnce   sync.Once
	loadedPath string
	loadErr    error
)

// Load tries to locate and load a .env file once per process.
// Search order:
// 1) ENV_FILE (if set)
// 2) .env walking up from current working directory
func Load() (string, error) {
	loadOnce.Do(func() {
		candidates := collectCandidates()
		for _, candidate := range candidates {
			if _, err := os.Stat(candidate); err != nil {
				continue
			}
			if err := godotenv.Load(candidate); err != nil {
				loadErr = err
				return
			}
			loadedPath = candidate
			return
		}
		loadErr = os.ErrNotExist
	})

	return loadedPath, loadErr
}

func collectCandidates() []string {
	candidates := make([]string, 0, 8)
	seen := make(map[string]struct{}, 8)

	add := func(path string) {
		if path == "" {
			return
		}
		clean := filepath.Clean(path)
		if _, ok := seen[clean]; ok {
			return
		}
		seen[clean] = struct{}{}
		candidates = append(candidates, clean)
	}

	if explicit := os.Getenv("ENV_FILE"); explicit != "" {
		add(explicit)
	}

	cwd, err := os.Getwd()
	if err == nil {
		dir := cwd
		for {
			add(filepath.Join(dir, ".env"))
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	return candidates
}

func IsNotFound(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}
