package sunduk

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"sort"
)

type entry struct {
	Offset int64
	Size   int64
}

type Sunduk struct {
	FilePath string // FilePath is the path to the file used to persist

	file   *os.File
	offset int64
	data   map[string][]byte
	index  map[string]entry
}

// New creates a new Sunduk
func New(filePath string) *Sunduk {
	store := &Sunduk{
		FilePath: filePath,
		data:     make(map[string][]byte),
		index:    make(map[string]entry),
	}
	err := store.loadFromDisk()
	if err != nil {
		panic(err)
	}
	return store
}

// Close closes the store's file if it isn't already closed.
// Note that any write actions, such as the usage of Put, PutAll or Delete, will automatically re-open the store
func (store *Sunduk) Close() {
	if store.file == nil {
		return
	}
	_ = store.file.Close()
	store.file = nil
}

// Get returns the value of a key as well as a bool that indicates whether an entry exists for that key
func (store *Sunduk) Get(key string) (value []byte, ok bool) {
	value, ok = store.data[key]
	return
}

// Put creates an entry or updates the value of an existing key
func (store *Sunduk) Put(key string, value []byte) error {
	store.index[key] = entry{0, int64(len(value))}
	store.data[key] = value
	return store.flush()
}

// PutAll creates or updates a map of entries
func (store *Sunduk) PutAll(entries map[string][]byte) error {
	for key, value := range entries {
		store.data[key] = value
		store.index[key] = entry{0, 0}
	}
	return store.flush()
}

// Delete removes a key from the store
func (store *Sunduk) Delete(key string) error {
	delete(store.data, key)
	return nil
}

// Count returns the total number of entries in the store
func (store *Sunduk) Count() int {
	length := len(store.data)
	return length
}

// Keys returns a list of all keys
func (store *Sunduk) Keys() []string {
	keys := make([]string, len(store.data))
	i := 0
	for k := range store.data {
		keys[i] = k
		i++
	}
	return keys
}

// loadFromDisk loads the store from the disk and consolidates the entries, or creates an empty file if there is no file
func (store *Sunduk) loadFromDisk() error {
	store.data = make(map[string][]byte)
	file, err := os.Open(store.FilePath)
	if err != nil {
		// Check if the file exists, if it doesn't, then create it and return
		if os.IsNotExist(err) {
			file, err := os.Create(store.FilePath)
			if err != nil {
				return err
			}
			store.file = file
			return nil
		} else {
			return err
		}
	}
	// File doesn't exist, so we need to read it
	store.file = file
	return nil
}

// flush combines all entries recorded in the file and re-saves only the necessary entries.
// The function is executed on creation, but can also be executed manually if storage space is a concern.
// The original file is backed up
func (store *Sunduk) flush() error {
	// Back up the old file before doing the flushing
	store.Close()
	bakname := store.FilePath + ".bak"
	err := os.Rename(store.FilePath, bakname)
	if err != nil {
		return fmt.Errorf(
			"unable to rename %s to %s during flushing: %s", store.FilePath, bakname, err.Error(),
		)
	}

	err = store.save()
	_ = os.Remove(bakname)
	if err != nil {
		_ = os.Rename(bakname, store.FilePath)
		return fmt.Errorf("unable to save new file at %s during flushing: %s", store.FilePath, err.Error())
	}

	_ = os.Remove(bakname)
	return nil
}

// save make physical saving data on disk
func (store *Sunduk) save() error {
	// Load all from disk
	//	for k, _ := range store.index {
	//		store.index[k] = entry{0, int64(len(store.data[k]))}
	//	}

	// Sort keys
	keys := make([]string, 0, len(store.index))
	for k := range store.index {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Pack data & Recalc offsets
	//var zb bytes.Buffer
	//zw, _ := flate.NewWriter(&zb, flate.DefaultCompression)
	//zw := gzip.NewWriter(&zb)
	var offset int64 = 0
	for _, k := range keys {
		var zb bytes.Buffer
		zw := gzip.NewWriter(&zb)
		v := store.data[k]
		_, _ = zw.Write(v)
		store.data[k] = zb.Bytes()
		//zb.Reset()
		//zw.Reset(&zb)

		store.index[k] = entry{offset, int64(len(store.data[k]))}
		offset += store.index[k].Size
	}

	//Save header
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(store.index)
	if err != nil {
		return err
	}

	var zbb bytes.Buffer
	gzw := gzip.NewWriter(&zbb)
	_, _ = gzw.Write(buf.Bytes())
	_ = store.file.Close()
	file, err := os.Create(store.FilePath)
	if err != nil {
		return err
	}
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(zbb.Len()))
	_, _ = file.Write(size[:])
	_, _ = file.Write(zbb.Bytes())

	for _, k := range keys {
		s, err := file.Write(store.data[k])
		if s == 0 || err != nil {
			return err
		}
	}

	return nil
}
