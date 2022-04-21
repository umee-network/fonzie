package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"
	log "github.com/sirupsen/logrus"
)

type ChainPrefix = string
type Username = string
type FundingReceipt struct {
	ChainPrefix ChainPrefix
	Username    Username
	FundedAt    time.Time
	Amount      cosmostypes.Coins
}
type FundingReceipts []FundingReceipt
type FundingReceiptsJson struct {
	Receipts string `json:"receipts" firestore:"receipts"`
}

// Db represents the application interface for accessing the database
type Db struct {
	Firestore *firestore.Client
}

var (
	db  *Db
	ctx context.Context
)

func init() {
	// Initialize Firestore
	client, err := initFirestore()
	if err != nil {
		log.Fatal(err)
	}
	db = &Db{
		Firestore: client,
	}
}

// ProvideFirestore returns a *firestore.Client
func initFirestore() (*firestore.Client, error) {
	ctx = context.Background()
	conf := &firebase.Config{ProjectID: os.Getenv("GCP_PROJECT")}

	app, err := firebase.NewApp(ctx, conf)
	if err != nil {
		return nil, err
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		return nil, err
	}

	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		log.Println("🚨 Production Firestore Host (cloud) is activated")
	} else {
		log.Println("🧪 Emulator Firestore Host is activated")
	}
	return client, nil
}

func (receipts *FundingReceipts) Add(newReceipt FundingReceipt) {
	*receipts = append(*receipts, newReceipt)
	// save to db
	err := db.SaveFundingReceipts(receipts)
	if err != nil {
		// GCP will restart pod
		log.Fatal(err)
	}
}

func (receipts *FundingReceipts) FindByChainPrefixAndUsername(prefix ChainPrefix, username Username) *FundingReceipt {
	for _, receipt := range *receipts {
		log.Info("FindByChainPrefixAndUsername: %v %v", receipt.ChainPrefix, receipt.Username)
		if receipt.ChainPrefix == prefix && receipt.Username == username {
			return &receipt
		}
	}
	return nil
}

func (receipts *FundingReceipts) Prune(maxAge time.Duration) {
	pruned := FundingReceipts{}
	now := time.Now()
	for _, receipt := range *receipts {
		if now.Before(receipt.FundedAt.Add(maxAge)) {
			pruned = append(pruned, receipt)
		}
	}
	*receipts = pruned
	// Save to db
	err := db.SaveFundingReceipts(receipts)
	if err != nil {
		// GCP will restart pod
		log.Fatal(err)
	}
}

func (db Db) RunTransaction(ctx context.Context, runner func(ctx context.Context, tx *firestore.Transaction) error) error {
	return db.Firestore.RunTransaction(ctx, runner)
}

func (db Db) SaveFundingReceipts(receipts *FundingReceipts) error {
	doc := db.Firestore.Doc("funding-receipts/receipts")
	bytes, err := json.Marshal(receipts)
	if err != nil {
		log.Error("marshal error %v", err)
		return err
	}
	wr, err := doc.Create(ctx, &FundingReceiptsJson{Receipts: string(bytes)})
	if err != nil {
		// Attempt to update the document
		wr, err = doc.Update(ctx, []firestore.Update{{Path: "Receipts", Value: string(bytes)}})
		if err != nil {
			return err
		}
	}
	log.Infof("Saved funding receipts: %v", wr)
	return nil
}

func (db Db) GetFundingReceipts() (*FundingReceipts, error) {
	receipts := &FundingReceipts{}
	doc := db.Firestore.Doc("funding-receipts/receipts")
	snap, err := doc.Get(ctx)
	if err != nil {
		return nil, err
	}
	var myReceipts FundingReceiptsJson
	if err := snap.DataTo(&myReceipts); err != nil {
		log.Info(err)
	}
	json.Unmarshal([]byte(myReceipts.Receipts), &receipts)
	log.Infof("Got funding receipts: %v", myReceipts.Receipts)
	return receipts, nil
}
