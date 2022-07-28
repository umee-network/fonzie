package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"os"
	"os/signal"
	"syscall"

	"github.com/bwmarrin/discordgo"
	"github.com/cosmos/btcutil/bech32"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"
	log "github.com/sirupsen/logrus"
	"github.com/umee-network/fonzie/chain"
	"github.com/umee-network/fonzie/customlens"
	"github.com/umee-network/fonzie/db"
)

//go:generate bash -c "if [ \"$CI\" = true ] ; then echo -n $GITHUB_REF_NAME > VERSION; fi"
var (
	//go:embed VERSION
	Version string
)

type CoinsStr = string
type FeesStr = string
type ChainFundingInfo struct {
	Coins CoinsStr `json:"coins"`
	Fees  FeesStr  `json:"fees"`
}
type ChainFunding = map[db.ChainPrefix]ChainFundingInfo

var (
	mnemonic           = os.Getenv("MNEMONIC")
	port               = os.Getenv("PORT")
	botToken           = os.Getenv("BOT_TOKEN")
	rawChains          = os.Getenv("CHAINS")
	rawFunding         = os.Getenv("FUNDING")
	rawFundingInterval = os.Getenv("FUNDING_INTERVAL")
	isSilent           = os.Getenv("SILENT") != ""
	funding            ChainFunding
	fundingInterval    time.Duration
	pruneMode          = false
)

func init() {
	foo := customlens.CustomChainClient{}
	log.Println(foo)
	if len(os.Args) > 1 {
		if os.Args[1] == "version" {
			fmt.Println(Version)
			os.Exit(0)
		}
		if os.Args[1] == "prune" {
			pruneMode = true
		}

	}

	if os.Getenv("ENABLE_JSON_LOGGING") == "true" || os.Getenv("ENABLE_JSON_LOGGING") == "1" {
		log.SetFormatter(&log.JSONFormatter{
			DisableHTMLEscape: true,
		})
	} else {
		log.SetFormatter(&log.TextFormatter{})
	}

	if port == "" {
		port = "8080"
		log.Printf("defaulting to port %s", port)
	}
	if mnemonic == "" {
		log.Fatal("MNEMONIC is invalid")
	}
	if botToken == "" {
		log.Fatal("BOT_TOKEN is invalid")
	}
	if rawChains == "" {
		log.Fatal("CHAINS cannot be blank (json array)")
	}
	if rawFunding == "" {
		log.Fatal("FUNDING cannot be blank (json array)")
	}
	if rawFundingInterval == "" {
		fundingInterval = time.Hour * 12
		log.Info("FUNDING_INTERVAL was not set and is defaulting to 12 hours")
	} else {
		var err error
		fundingInterval, err = time.ParseDuration(rawFundingInterval)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func initChains() chain.Chains {
	// parse chains config
	var chains chain.Chains
	err := json.Unmarshal([]byte(rawChains), &chains)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("CHAINS: %#v", chains)
	// parse chains config
	err = json.Unmarshal([]byte(rawFunding), &funding)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("CHAIN_FUNDING: %#v", funding)
	return chains
}

func main() {
	ctx := context.Background()
	db := db.NewDb(ctx)

	if pruneMode {
		numPruned, err := db.PruneExpiredReceipts(ctx, time.Now().Add(-fundingInterval))
		if err != nil {
			log.Fatal(err)
		}
		log.Infof("pruned %d receipts", numPruned)
		os.Exit(0)
	}

	chains := initChains()

	err := chains.ImportMnemonic(ctx, mnemonic)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new Discord session using the provided bot token.
	dg, err := discordgo.New("Bot " + botToken)
	if err != nil {
		log.Fatal(err)
	}
	defer dg.Close()

	fh := NewFaucetHandler(chains, db)
	dg.AddHandler(fh.handleDispense)

	// we only care about receiving message events.
	dg.Identify.Intents = discordgo.IntentsGuildMessages
	// dg.Identify.Intents = discordgo.IntentDirectMessages

	// Open a websocket connection to Discord and begin listening.
	err = dg.Open()
	if err != nil {
		log.Fatal(err)
	}

	// Create http endpoint for faucet requests
	http.HandleFunc("/", fh.faucetHttp)
	log.Printf("listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}

	// Wait here until CTRL-C or other term signal is received.
	if isSilent {
		log.Info("SILENT MODE:  The Fonz is still running, only reporting errors.  Press CTRL-C to exit.")
	} else {
		log.Info("The Fonz bot is now thumbs-up'ing.  Press CTRL-C to exit.")
	}
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	<-sc
}

type FaucetHandler struct {
	faucets map[string]ChainFaucet
	quit    chan bool
	chains  chain.Chains
	db      db.Db
	ctx     context.Context

	cmd *regexp.Regexp
}

func NewFaucetHandler(chains chain.Chains, db db.Db) FaucetHandler {
	re, err := regexp.Compile("!(request|help)(.*)")
	if err != nil {
		log.Fatal(err)
	}
	var faucets = make(map[string]ChainFaucet)
	var quit = make(chan bool)
	for _, c := range chains {
		f := ChainFaucet{make(chan FaucetReq), c}
		faucets[c.Prefix] = f
		go f.Consume(quit)
	}
	return FaucetHandler{
		faucets: faucets,
		quit:    quit,
		chains:  chains,
		cmd:     re,
		ctx:     context.Background(),
		db:      db,
	}
}

func (fh FaucetHandler) faucetHttp(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if !query.Has("wallet") {
		httpError(w, "wallet is required")
		return
	}
	wallet := strings.TrimSpace(query.Get("wallet"))
	prefix, _, err := bech32.Decode(wallet, 1023)
	if err != nil {
		httpError(w, prefix+" is not supported: "+err.Error())
		return
	}

	faucet, ok := fh.faucets[prefix]
	if !ok {
		httpError(w, prefix+" is not supported"+err.Error())
		return
	}
	coins, err := cosmostypes.ParseCoinsNormalized(funding[prefix].Coins)
	if err != nil {
		httpError(w, "parsing coins: "+err.Error())
		return
	}
	fees, err := cosmostypes.ParseCoinsNormalized(funding[prefix].Fees)
	if err != nil {
		httpError(w, "parsing fees: "+err.Error())
		return
	}

	ipAddress := r.RemoteAddr
	log.Infof("request from %s", ipAddress)
	receipt, err := fh.db.GetFundingReceiptByUsernameAndChainPrefix(fh.ctx, ipAddress, prefix)
	if err != nil {
		httpError(w, err.Error())
		return
	}

	if receipt != nil {
		log.Infof("FETCHED RECEIPT RESULT: %#v", receipt.FundedAt.Add(fundingInterval).After(time.Now()))
	}

	if receipt != nil && receipt.FundedAt.Add(fundingInterval).After(time.Now()) {
		httpError(w, (fmt.Errorf("you must wait %v until you can get %s funding again", time.Until(receipt.FundedAt.Add(fundingInterval)).Round(2*time.Second), prefix).Error()))
		return
	}

	recipient, err := faucet.chain.DecodeAddr(wallet)
	if err != nil {
		httpError(w, "malformed destination address: "+err.Error())
		return
	}

	faucet.channel <- FaucetReq{recipient, coins, fees, nil, nil}
	err = fh.db.SaveFundingReceipt(fh.ctx, db.FundingReceipt{
		ChainPrefix: prefix,
		Username:    ipAddress,
		FundedAt:    time.Now(),
		Amount:      coins,
	})
	if err != nil {
		log.Error(err)
	}
	// success
	fmt.Fprintf(w, "%s faucet tokens sent to wallet: %s", prefix, wallet)
}

func httpError(w http.ResponseWriter, err string) {
	w.Header().Set("x-faucet-error", err)
	log.Errorf(err)
	defer w.WriteHeader(http.StatusBadRequest)
}

// This function will be called (due to AddHandler above) every time a new
// message is created on any channel that the authenticated bot has access to.
func (fh FaucetHandler) handleDispense(s *discordgo.Session, m *discordgo.MessageCreate) {
	// Ignore all messages created by the bot itself
	// This isn't required in this specific example but it's a good practice.
	if m.Author.ID == s.State.User.ID {
		return
	}

	// Do we support this bech32 prefix?
	matches := fh.cmd.FindAllStringSubmatch(m.Content, -1)
	if len(matches) > 0 {
		// for each matched request, do--
		for _, match := range matches {
			cmd := strings.TrimSpace(match[1])
			args := strings.TrimSpace(match[2])
			switch cmd {
			case "request":
				// TODO if role doesn't exist, reply with help and return
				// - "umeemaniac"
				// - ROLE_REQUIRED="role string/id", optional from env
				dstAddr := args
				prefix, _, err := bech32.Decode(dstAddr, 1023)
				if err != nil {
					reportError(s, m, err)
					return
				}

				faucet, ok := fh.faucets[prefix]
				if !ok {
					reportError(s, m, fmt.Errorf("%s chain prefix is not supported", prefix))
					return
				}
				coins, err := cosmostypes.ParseCoinsNormalized(funding[prefix].Coins)
				if err != nil {
					reportError(s, m, err)
					return
				}
				fees, err := cosmostypes.ParseCoinsNormalized(funding[prefix].Fees)
				if err != nil {
					reportError(s, m, err)
					return
				}

				receipt, err := fh.db.GetFundingReceiptByUsernameAndChainPrefix(fh.ctx, m.Author.ID, prefix)
				if err != nil {
					log.Error(err)
					return
				}

				if receipt != nil {
					log.Infof("FETCHED RECEIPT RESULT: %#v", receipt.FundedAt.Add(fundingInterval).After(time.Now()))
				}

				if receipt != nil && receipt.FundedAt.Add(fundingInterval).After(time.Now()) {
					reportError(s, m, fmt.Errorf("you must wait %v until you can get %s funding again", time.Until(receipt.FundedAt.Add(fundingInterval)).Round(2*time.Second), prefix))
					return
				}

				recipient, err := faucet.chain.DecodeAddr(dstAddr)
				if err != nil {
					reportError(s, m, fmt.Errorf("malformed destination address, err: %w", err))
					return
				}

				// Immediately respond to Discord
				sendReaction(s, m, "👍")
				faucet.channel <- FaucetReq{recipient, coins, fees, s, m}

				err = fh.db.SaveFundingReceipt(fh.ctx, db.FundingReceipt{
					ChainPrefix: prefix,
					Username:    m.Author.ID,
					FundedAt:    time.Now(),
					Amount:      coins,
				})
				if err != nil {
					log.Error(err)
				}

			default:
				help(s, m, fh.chains)
			}
		}
	} else if m.GuildID == "" {
		// If message is DM, respond with help
		help(s, m, fh.chains)
	}
}

func reportError(s *discordgo.Session, m *discordgo.MessageCreate, errToReport error) {
	if m.Author.Bot {
		// guard against known bots
		return
	}
	err := sendReaction(s, m, "❌")
	if err != nil {
		log.Error(err)
	}
	// Send errors to channel, even when isSilent
	_, err = s.ChannelMessageSendReply(m.ChannelID, fmt.Sprintf("<@%s>, there is an error in your request:\n `%s`", m.Author.ID, errToReport), m.Reference())
	if err != nil {
		log.Error(err)
	}
}

//go:embed help.md
var helpMsg string

func help(s *discordgo.Session, m *discordgo.MessageCreate, chains chain.Chains) {
	acc := []string{}
	for _, chain := range chains {
		acc = append(acc, chain.Prefix)
	}
	err := sendMessage(s, m, fmt.Sprintf("**Supported address prefixes**: %s.\n\n%s", strings.Join(acc, ", "), helpMsg))
	if err != nil {
		log.Error(err)
	}
}

func isDM(m *discordgo.MessageCreate) bool {
	return m.GuildID == ""
}

func sendMessage(s *discordgo.Session, m *discordgo.MessageCreate, msg string) error {
	if s == nil || m == nil {
		return nil
	}
	if m.Author.Bot || isSilent && !isDM(m) {
		// Silent mode is enabled, so-- only reply to DMs
		return nil
	}
	directMessageChannel, err := s.UserChannelCreate(m.Author.ID)
	if err != nil {
		return err
	}
	_, err = s.ChannelMessageSend(directMessageChannel.ID, msg)
	if err != nil {
		return err
	}
	return nil
}

func sendReaction(s *discordgo.Session, m *discordgo.MessageCreate, reaction string) error {
	if m == nil || s == nil {
		return nil
	}
	if isSilent || m.Author.Bot {
		return nil
	}
	return s.MessageReactionAdd(m.ChannelID, m.ID, reaction)
}
