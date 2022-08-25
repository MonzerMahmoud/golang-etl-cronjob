package main

import (
	"time"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"

	"github.com/go-co-op/gocron"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	log "github.com/sirupsen/logrus"
)

var DB *gorm.DB

type User struct {
	gorm.Model
	FullName string
	Email    string
}

type Transaction struct {
	gorm.Model
	TransactionType string
	Amount          float64
	UserID          uint
}

type AnalyticsTransactions struct {
	TransactionID uint
	TransactionType string
	Amount          string
	UserID          uint
	UserFullName    string
	UserEmail       string
}

func InitDatabase() {
	log.Info("Initializing database")

	const DBURL = "host=localhost port=5432 user=postgres dbname=testingDB sslmode=disable"

	database, err := gorm.Open("postgres", DBURL)

	if err != nil {
		log.Fatal(err)
	}

	database.AutoMigrate(&Transaction{}, &User{})

	DB = database
}


func ExtractTransactionData() []Transaction {
	log.Info("Extracting Transaction data from prodcution DB")

	var Transactions = []Transaction{}
	DB.Find(&Transactions)

	log.Info("Extracted Transaction data from prodcution DB")
	return Transactions
}

func TransformingTransactionData(transactions []Transaction) []AnalyticsTransactions {
	log.Info("Transforming transaction data")

	var analyticsTransactions = []AnalyticsTransactions{}

	for _, transaction := range transactions {
		user := User{}
		DB.Where("id = ?", transaction.UserID).First(&user)
		analyticsTransactions = append(analyticsTransactions, AnalyticsTransactions{
			TransactionID: transaction.ID,
			TransactionType: transaction.TransactionType,
			Amount:          fmt.Sprintf("%.2f SDG", transaction.Amount),
			UserID:          transaction.UserID,
			UserFullName:    user.FullName,
			UserEmail:       user.Email,
		})
	}

	fmt.Println(analyticsTransactions)
	log.Info("Transformed transaction data")

	return analyticsTransactions

}

func LoadingTransactionData(analyticsTransactions []AnalyticsTransactions) {
	log.Info("Loading transaction data to csv file")

	transactionsFile, err := os.Create("transactions.csv")

	if err != nil {
		log.Fatal(err)
	}

	defer transactionsFile.Close()

	w := csv.NewWriter(transactionsFile)
    defer w.Flush()

	header := []string{"TransactionID", "TransactionType", "Amount", "UserFullName", "UserEmail", "UserID"}
	w.Write(header)
	
	var data [][]string
	for _, transaction := range analyticsTransactions {
		row := []string{strconv.Itoa(int(transaction.TransactionID)), transaction.TransactionType, transaction.Amount, transaction.UserFullName, transaction.UserEmail, strconv.Itoa(int(transaction.UserID))}
		data = append(data, row)
	}
	w.WriteAll(data)

	log.Info("Loaded transaction data to csv file")

}

func TransactionETL() {
	log.Info("Starting tranactions ETL job")
	LoadingTransactionData(TransformingTransactionData(ExtractTransactionData()))
}

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	InitDatabase()

	s := gocron.NewScheduler(time.UTC)
	log.Info("Starting scheduler")
	s.Every(10).Seconds().Do(TransactionETL)
	//s.Every(1).Day().At("12:08").Do(TransactionETL)

	s.StartAsync()

	s2 := gocron.NewScheduler(time.UTC)
	s2.StartBlocking()

}

// func main() {
// 	fmt.Println("new cron")
// 	s := gocron.NewScheduler(time.UTC)
// 	fmt.Println("new task added")
// 	s.Every(5).Second().Do(task)
// 	s.StartAsync()
// }

// func task() {
// 	fmt.Println("task")
// 	time.Sleep(2 * time.Second)
// }
