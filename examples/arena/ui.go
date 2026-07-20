package main

import (
	"embed"
	"net/http"

	"github.com/dhamidi/htmlc"
)

//go:embed index.html
var indexHTML []byte

//go:embed components
var componentFS embed.FS

var editorEngine = func() *htmlc.Engine {
	engine, err := htmlc.New(htmlc.Options{FS: componentFS, ComponentDir: "components"})
	if err != nil {
		panic("initialize arena editor components: " + err.Error())
	}
	return engine
}()

func indexHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(indexHTML)
}

func editorHandler(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if err := editorEngine.RenderPage(request.Context(), w, "BotEditorPage", nil); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
