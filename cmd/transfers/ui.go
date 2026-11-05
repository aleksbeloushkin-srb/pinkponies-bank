package main

import (
	"io/fs"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/web"
)

// staticUI returns the embedded web UI.
func staticUI() fs.FS { return web.FS }
