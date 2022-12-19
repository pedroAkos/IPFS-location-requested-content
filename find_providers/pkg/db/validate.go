package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"unicode/utf8"
)

// checkIfValidString checks if a string is valid utf8
func checkIfValidString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	} else {
		if !utf8.Valid([]byte(s)) {
			panic(fmt.Sprintf("String %v is not valid utf8", s))
		}
		return sql.NullString{
			String: s,
			Valid:  true,
		}
	}
}

// checkIfValidInt checks if a string has size 0 to return a valid null int
func checkIfValidInt(s string) sql.NullInt32 {
	if len(s) == 0 {
		return sql.NullInt32{}
	} else {
		i, err := strconv.Atoi(s)
		if err != nil {
			return sql.NullInt32{}
		}
		return sql.NullInt32{
			Int32: int32(i),
			Valid: true,
		}
	}
}

// checkIfValidFloat checks if a string has size 0 to return a valid null float
func checkIfValidFloat(s string) sql.NullFloat64 {
	if len(s) == 0 {
		return sql.NullFloat64{}
	} else {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return sql.NullFloat64{}
		}
		return sql.NullFloat64{
			Float64: f,
			Valid:   true,
		}
	}
}
