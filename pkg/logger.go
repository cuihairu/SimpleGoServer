package pkg

type Logger interface {
	Print(v ...any)
	Println(v ...any)
	Printf(format string, v ...any)
	Fatal(v ...any)
	Fatalf(format string, v ...any)
	Panic(v ...any)
}
