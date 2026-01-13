package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "golang.org/x/image/webp"

	"github.com/corona10/goimagehash"
	"github.com/rwcarlsen/goexif/exif"
)

var imageExts = map[string]bool{
	".jpg":  true,
	".jpeg": true,
	".png":  true,
	".gif":  true,
	".webp": true,
	".heic": true,
	".heif": true,
}

// IO job → read file
type IOJob struct {
	Path string
	Info os.FileInfo
}

// CPU job → compute hashes
type CPUJob struct {
	Path string
	Info os.FileInfo
	Data []byte
}

// CPU result → dedupe & sorting
type HashResult struct {
	Path string
	Info os.FileInfo
	Sha  string
	Ph   *goimagehash.ImageHash
	Err  error
}

// Progress counters
var totalFiles int64
var processedFiles int64
var startTime time.Time

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <folder>")
	}

	src := os.Args[1]
	dupFolder := filepath.Join(src, "duplicates")
	if err := os.MkdirAll(dupFolder, 0755); err != nil {
		log.Fatalf("mkdir duplicates: %v", err)
	}

	numCPU := runtime.NumCPU()
	fmt.Printf("Using %d threads based on CPU cores\n", numCPU)

	// Dedup maps
	var seenExact sync.Map // sha256 → path
	var phMutex sync.Mutex
	seenPHash := make(map[*goimagehash.ImageHash]string)

	// Channels
	ioJobs := make(chan IOJob, 200)
	cpuJobs := make(chan CPUJob, 200)
	results := make(chan HashResult, 200)

	// ========= FIRST PASS: count files =========
	if err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(info.Name()))
		if imageExts[ext] {
			atomic.AddInt64(&totalFiles, 1)
		}
		return nil
	}); err != nil {
		log.Fatalf("walk count: %v", err)
	}

	startTime = time.Now()

	// ========= IO worker pool =========
	var wgIO sync.WaitGroup
	ioWorkers := numCPU
	wgIO.Add(ioWorkers)
	for i := 0; i < ioWorkers; i++ {
		go func() {
			ioWorker(ioJobs, cpuJobs)
			wgIO.Done()
		}()
	}

	// Close cpuJobs when all IO workers finish
	go func() {
		wgIO.Wait()
		close(cpuJobs)
	}()

	// ========= CPU worker pool =========
	var wgCPU sync.WaitGroup
	cpuWorkers := numCPU
	wgCPU.Add(cpuWorkers)
	for i := 0; i < cpuWorkers; i++ {
		go func() {
			cpuWorker(cpuJobs, results)
			wgCPU.Done()
		}()
	}

	// Close results when all CPU workers finish
	go func() {
		wgCPU.Wait()
		close(results)
	}()

	// ========= PROGRESS BAR =========
	go progressBar()

	// ========= SECOND PASS: feed jobs =========
	go func() {
		filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			ext := strings.ToLower(filepath.Ext(info.Name()))
			if imageExts[ext] {
				ioJobs <- IOJob{Path: path, Info: info}
			}
			return nil
		})
		close(ioJobs)
	}()

	// ========= RESULT CONSUMER =========
	for res := range results {
		atomic.AddInt64(&processedFiles, 1)

		// Skip any file already in duplicates folder
		if strings.HasPrefix(strings.ToLower(res.Path), strings.ToLower(dupFolder)+string(os.PathSeparator)) {
			continue
		}

		if res.Err != nil {
			fmt.Printf("Error: %s → %v\n", res.Path, res.Err)
			continue
		}

		// ---- Exact duplicate detection (case-insensitive path) ----
		if prev, ok := seenExact.LoadOrStore(res.Sha, strings.ToLower(res.Path)); ok {
			fmt.Printf("EXACT DUPLICATE: %s → %s\n", res.Path, prev.(string))
			if err := moveToDuplicates(dupFolder, res.Path, res.Info); err != nil {
				fmt.Printf("move dup error: %v\n", err)
			}
			continue
		}

		// ---- Near duplicate detection (pHash) ----
		var nearDup string
		phMutex.Lock()
		for existingPh, existingPath := range seenPHash {
			dist, derr := res.Ph.Distance(existingPh)
			if derr == nil && dist <= 5 {
				nearDup = existingPath
				break
			}
		}
		if nearDup == "" {
			seenPHash[res.Ph] = strings.ToLower(res.Path)
		}
		phMutex.Unlock()

		if nearDup != "" {
			fmt.Printf("NEAR DUPLICATE: %s → %s\n", res.Path, nearDup)
			if err := moveToDuplicates(dupFolder, res.Path, res.Info); err != nil {
				fmt.Printf("move dup error: %v\n", err)
			}
			continue
		}

		// finally, sort the unique photo
		if err := sortPhoto(src, res.Path, res.Info); err != nil {
			fmt.Printf("sort error: %v\n", err)
		}
	}

	fmt.Println("\nAll done!")
}

///////////////////////////////////////////////////////////
// IO WORKER
///////////////////////////////////////////////////////////
func ioWorker(in <-chan IOJob, out chan<- CPUJob) {
	for job := range in {
		ext := strings.ToLower(filepath.Ext(job.Info.Name()))
		if ext == ".heic" || ext == ".heif" {
			jpgPath, err := convertHEICtoJPG(job.Path)
			if err != nil {
				fmt.Printf("HEIC convert error: %v\n", err)
				out <- CPUJob{Path: job.Path, Info: job.Info, Data: nil}
				continue
			}

			fmt.Printf("CONVERT: %s → %s\n", job.Path, jpgPath)

			info, err := os.Stat(jpgPath)
			if err != nil {
				fmt.Printf("stat error: %s → %v\n", jpgPath, err)
				out <- CPUJob{Path: jpgPath, Info: job.Info, Data: nil}
				continue
			}

			data, err := os.ReadFile(jpgPath)
			if err != nil {
				fmt.Printf("read error: %s → %v\n", jpgPath, err)
				out <- CPUJob{Path: jpgPath, Info: info, Data: nil}
				continue
			}

			out <- CPUJob{Path: jpgPath, Info: info, Data: data}
			continue
		}

		// Normal images
		data, err := os.ReadFile(job.Path)
		if err != nil {
			fmt.Printf("read error: %s → %v\n", job.Path, err)
			out <- CPUJob{Path: job.Path, Info: job.Info, Data: nil}
			continue
		}
		out <- CPUJob{Path: job.Path, Info: job.Info, Data: data}
	}
}

///////////////////////////////////////////////////////////
// CPU WORKER
///////////////////////////////////////////////////////////
func cpuWorker(in <-chan CPUJob, out chan<- HashResult) {
	for job := range in {
		if job.Data == nil {
			out <- HashResult{Path: job.Path, Info: job.Info, Err: fmt.Errorf("no data or unsupported format")}
			continue
		}

		sha := fmt.Sprintf("%x", sha256.Sum256(job.Data))

		img, derr := safeDecodeFromBytes(job.Path, job.Data)
		if derr != nil {
			out <- HashResult{Path: job.Path, Info: job.Info, Err: derr}
			continue
		}

		ph, perr := goimagehash.PerceptionHash(img)
		if perr != nil {
			out <- HashResult{Path: job.Path, Info: job.Info, Err: perr}
			continue
		}

		out <- HashResult{Path: job.Path, Info: job.Info, Sha: sha, Ph: ph}
	}
}

///////////////////////////////////////////////////////////
// SAFE DECODE
///////////////////////////////////////////////////////////
func safeDecodeFromBytes(path string, data []byte) (image.Image, error) {
	reader := bytes.NewReader(data)
	img, _, err := image.Decode(reader)
	if err != nil {
		return nil, fmt.Errorf("decode failed (%s): %w", filepath.Base(path), err)
	}
	return img, nil
}

///////////////////////////////////////////////////////////
// HEIC → JPG via CLI
///////////////////////////////////////////////////////////
func convertHEICtoJPG(srcPath string) (string, error) {
	jpgPath := strings.TrimSuffix(srcPath, filepath.Ext(srcPath)) + ".jpg"
	cmd := exec.Command("heif-convert", srcPath, jpgPath)
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("heif-convert failed: %w", err)
	}

	if err := os.Remove(srcPath); err != nil {
		return "", fmt.Errorf("failed to delete original HEIC: %w", err)
	}
	return jpgPath, nil
}

///////////////////////////////////////////////////////////
// PROGRESS BAR
///////////////////////////////////////////////////////////
func progressBar() {
	for {
		done := atomic.LoadInt64(&processedFiles)
		total := atomic.LoadInt64(&totalFiles)

		if total == 0 {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		percent := float64(done) / float64(total)
		elapsed := time.Since(startTime)
		var eta time.Duration
		if done > 0 && percent > 0 {
			eta = time.Duration(float64(elapsed) * (1/percent - 1))
		}

		width := 40
		filled := int(percent * float64(width))
		if filled > width {
			filled = width
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)

		fmt.Printf("\r[%s] %3.0f%%  %d/%d  ETA: %s    ",
			bar, percent*100, done, total, eta.Truncate(time.Second))

		if done >= total && total > 0 {
			fmt.Println()
			return
		}

		time.Sleep(200 * time.Millisecond)
	}
}

///////////////////////////////////////////////////////////
// Sorting & Duplicate Movement
///////////////////////////////////////////////////////////
func moveToDuplicates(dupFolder, path string, info os.FileInfo) error {
	dest := filepath.Join(dupFolder, strings.ToLower(info.Name()))
	if _, err := os.Stat(dest); err == nil {
		dest = filepath.Join(dupFolder, fmt.Sprintf("%d_%s", time.Now().Unix(), strings.ToLower(info.Name())))
	}

	absPath, _ := filepath.Abs(path)
	absDest, _ := filepath.Abs(dest)
	if absPath == absDest {
		// Already in duplicates folder
		return nil
	}

	return os.Rename(path, dest)
}

func sortPhoto(src, path string, info os.FileInfo) error {
	date, err := extractDate(path, info)
	if err != nil {
		date = info.ModTime()
	}

	year := date.Format("2006")
	month := date.Format("01")
	destDir := filepath.Join(src, year, month)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", destDir, err)
	}

	targetName := strings.ToLower(info.Name())
	dest := filepath.Join(destDir, targetName)

	// Skip move if already in the right folder
	absPath, _ := filepath.Abs(path)
	absDest, _ := filepath.Abs(dest)
	if absPath == absDest {
		return nil
	}

	// Handle collision
	if _, err := os.Stat(dest); err == nil {
		dest = filepath.Join(destDir, fmt.Sprintf("%d_%s", time.Now().Unix(), targetName))
	}

	fmt.Printf("MOVE: %s → %s\n", path, dest)
	if err := os.Rename(path, dest); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

func extractDate(path string, info os.FileInfo) (time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return info.ModTime(), err
	}
	defer f.Close()

	x, err := exif.Decode(f)
	if err != nil {
		return info.ModTime(), err
	}

	return x.DateTime()
}
