package utils

import "regexp"

func IsAlpha(str string) bool{
	r, _ := regexp.Compile("[a-z0-9A-Z]+")
	return r.MatchString(str)
}

