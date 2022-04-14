package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
)

func parseJson(dat []byte) {
	data := make(map[string]interface{})
	err := json.Unmarshal([]byte(dat), &data)
	if err != nil {
		fmt.Println(err)
	}
	traverseCheck(data)
}


func traverseCheck(el map[string]interface{}) {
	for key, val := range el{
		_ = val
		if key == "==" || key == ">=" || key == "<=" || key == "<" || key == ">"{
			//fmt.Println("****************")
			tmpMap := el[key].(map[string]interface{})
			fmt.Println(key)
			for k, v := range tmpMap{
				fmt.Println(k, v)
			}
			//fmt.Println("****************")

		} else if key == "||" || key == "&&"{
			tmpSlice := el[key].([]interface{})
			fmt.Println(key)
			for _, obj := range tmpSlice{
				data := obj.(map[string]interface{})
				traverseCheck(data)
			}
		}
	}
}

func TestParseJson(t *testing.T) {

	dat, err := ioutil.ReadFile("./example.json")
	if err != nil {
		fmt.Println(err)
	}
	parseJson(dat)
}