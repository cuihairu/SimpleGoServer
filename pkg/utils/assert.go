package utils

func AssertWriteLength(len any, err error) {
	if err != nil {
		panic(err)
	}
}
