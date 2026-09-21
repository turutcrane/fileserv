package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	// "github.com/fsnotify/fsnotify"
	"github.com/fswatcher/fswatcher"
	"github.com/turutcrane/haat"
	graceful "github.com/turutcrane/http_graceful"
	"golang.org/x/net/html/atom"

	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/html"
	"github.com/gomarkdown/markdown/parser"
)

var watcher *fswatcher.Watcher

type SSEBroker struct {
	*Broker[string]
}

func NewSSEBroker() *SSEBroker {
	b := &SSEBroker{NewBroker[string]()}
	return b
}

func (b *SSEBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher) // Assert writer implements Flusher

	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "text/event-stream")

	// Register client with the broker
	ch := b.Subscribe()
	defer b.Unsubscribe(ch)

	// Listen for events
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				// Channel has been closed
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", msg); err != nil {
				return
			}
			slog.Debug("http response written", "data", msg)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func main() {
	// tcp()
	// dumpHtml()
	var err error

	// prefixPath := flag.String("prefix", "", "/prefix/ path")
	dirpath := flag.String("path", ".", "path dir")
	portNo := flag.String("port", "8080", "port")
	flag.Parse()

	watcher, err = fswatcher.NewWatcher()
	if err != nil {
		log.Fatalln("T34:", err)
	}
	defer watcher.Close()

	if st, err := os.Lstat(*dirpath); err == nil {
		if !st.IsDir() {
			log.Fatalln(*dirpath, "is not a directory")
		}
	} else {
		log.Fatalln("T42:", err)
	}

	// Watch the directory, not the file itself.
	err = watcher.Add(*dirpath, fswatcher.Create|fswatcher.Write)
	if err != nil {
		log.Fatalln("T40:", err)
	}

	signalCtx, stop, waitShutdown := graceful.Context(context.Background(), 10*time.Second)
	defer stop()
	mux := http.NewServeMux()
	fsHander := rootFsHander(*dirpath)
	mux.Handle("/", fsHander)
	mux.Handle("/favicon.ico", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	mux.Handle("/.well-known/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	// mux.Handle("/ws", http.HandlerFunc(wsHandler))
	broker := NewSSEBroker()
	mux.Handle("/events", broker)
	go func() {
		for {
			select {
			case <-signalCtx.Done():
				log.Println("T56: signalCtx.Done")
				return
			case event, ok := <-watcher.Events:
				if !ok {
					log.Println("T103: watcher.Events. not ok")
					return
				}
				log.Println("T144: event:", event)
				if event.Op.Has(fswatcher.Write | fswatcher.Chmod) {
					broker.Broadcast(fmt.Sprintf("Modified: %s", event.Name))
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					log.Println("T103: watcher.Errors. not ok")
					return
				}
				log.Println("T165: error:", err)

			}
		}
	}()

	server := &http.Server{
		Addr:        ":" + *portNo,
		Handler:     mux,
		BaseContext: func(_ net.Listener) context.Context { return signalCtx },
	}

	// var wg sync.WaitGroup

	go func() {
		log.Println("starting server at :", *portNo)
		err := server.ListenAndServe()
		log.Println("T68: Listener Down:", err)
	}()

	waitShutdown(server)
	// wg.Wait()
}

func tcp() {
	port := 8080
	l, err := net.ListenTCP("tcp", &net.TCPAddr{Port: int(port)})
	if err != nil {
		log.Panic(err)
	}
	conn, err := l.SyscallConn()
	if err != nil {
		log.Panic(err)
	}
	conn.Control(func(fd uintptr) {
		log.Println("T100: fd:", fd)
	})
}

func rootFsHander(dirPath string) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var root *os.Root
		var err error
		if root, err = os.OpenRoot(dirPath); err != nil {
			log.Panicln("T275:", err)
		}
		defer root.Close()

		p := r.URL.Path
		if p == "/" {
			p = "."
		} else {
			p = strings.TrimPrefix(p, "/")

		}
		if s, err := root.Stat(p); err == nil {
			if s.IsDir() {
				head := haat.E(atom.Head)
				body := haat.E(atom.Body)
				if d, err := fs.ReadDir(root.FS(), p); err == nil {
					for _, e := range d {
						pe := haat.E(atom.P)
						u := url.URL{
							Path: path.Join(r.URL.Path, e.Name()),
						}
						a := haat.E(atom.A).SetA(haat.AttrHref(u))
						a.SetText(e.Name())
						pe.C(a)
						body.AppendC(pe)
					}
				} else {
					log.Panicln("T294:", err)
				}
				d := haat.NewDocument(head, body)
				d.Render(w)
			} else {
				if b, err := fs.ReadFile(root.FS(), p); err == nil {
					if strings.HasSuffix(p, ".css") {
						w.Header().Set("Content-Type", "text/css")
					}
					if strings.HasSuffix(p, ".md") {
						w.Write(mdToHTML(b))
					} else {
						w.Write(b)
					}
				} else {
					log.Panicln("T305:", err)
				}
			}
		} else {
			log.Panicln("T238:", err, r.URL.Path)
		}

	})
}

func dumpHtml() {
	if h, err := haat.ParseHTML(strings.NewReader(`
<!doctype html>
<html>
<head>
<title>Hello haat</title>
</head>
<body>
Hello <span id="pkgname"></span>!!
</body>
</html>
`)); err == nil {
		haat.DumpDocument(h, 4, "$")
	} else {
		log.Panicln("T339:", err)
	}
}

func mdToHTML(md []byte) []byte {
	// create markdown parser with extensions
	p := parser.NewWithExtensions(parser.CommonExtensions | parser.AutoHeadingIDs | parser.NoEmptyLineBeforeBlock)
	doc := p.Parse(md)

	// create HTML renderer with extensions
	opts := html.RendererOptions{Flags: html.CommonFlags | html.HrefTargetBlank}
	renderer := html.NewRenderer(opts)

	return markdown.Render(doc, renderer)
}
