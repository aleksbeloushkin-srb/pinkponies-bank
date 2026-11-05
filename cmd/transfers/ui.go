package main

import "io/fs"

// staticUI returns the web UI; nil until the UI is added.
func staticUI() fs.FS { return nil }
