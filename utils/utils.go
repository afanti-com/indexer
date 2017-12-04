package utils

import (
	"regexp"
	"math"
)


type DocInfo struct {
	Id int
	DocLen int
	WordlistLen int
}


func IsAlpha(str string) bool{
	r, _ := regexp.Compile("[a-z0-9A-Z]+")
	return r.MatchString(str)
}


func ScoreIdf(total uint32, df uint32) float64 {
	return math.Log((float64(total - df) + 0.5) / (float64(df) + 0.5));
}

