package packer

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func ReadStockPriceFile() []float64 {

	prices := make([]float64, 0, 5000000)

	// Open the stock price file in the data directory
	file, err := os.Open("/tmp/Stocks-Germany-sample.txt")
	if err != nil {
		return nil
	}

	defer file.Close()

	// Every line in the file is comma-delmited and has the following format:
	// <date>,<time>,<price>,<volume>
	// Read the stock price file and return the values
	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ",")
		// Convefrt the price to a float64
		price := 0.0
		_, err := fmt.Sscanf(parts[2], "%f", &price)
		if err != nil {
			return nil
		}
		prices = append(prices, price)
	}

	return prices
}
