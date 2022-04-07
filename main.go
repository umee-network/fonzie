package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"os"
	"os/signal"
	"syscall"

	"github.com/bwmarrin/discordgo"
	cosmostypes "github.com/cosmos/cosmos-sdk/types"
	log "github.com/sirupsen/logrus"
	chain "github.com/umee-network/fonzie/chain"
)

var mnemonic = os.Getenv("MNEMONIC")
var botToken = os.Getenv("BOT_TOKEN")
var rawChains = os.Getenv("CHAINS")
var chains chain.Chains

func init() {
	log.SetFormatter(&log.JSONFormatter{})

	if mnemonic == "" {
		log.Fatal("MNEMONIC is invalid")
	}
	if botToken == "" {
		log.Fatal("BOT_TOKEN is invalid")
	}
	if rawChains == "" {
		log.Fatal("CHAINS config cannot be blank (json array)")
	}
	// parse chains config
	err := json.Unmarshal([]byte(rawChains), &chains)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%#v", chains)
}

func main() {
	ctx := context.Background()
	err := chains.ImportMnemonic(ctx, mnemonic)
	if err != nil {
		log.Fatal(err)
	}

	// dstAddr := "umee1p7hp3dt94n83cn8xwvuz3lew9wn7kh04gkywdx"
	// coins := cosmostypes.NewCoins(cosmostypes.NewCoins(
	// 	cosmostypes.NewCoin("uumee", cosmostypes.NewInt(100000000)),
	// )...)
	// prefix := "umee"

	// chain := chains.FindByPrefix(prefix)
	// if chain == nil {
	// 	log.Fatalf("%s prefix is not supported", prefix)
	// }

	// err = chain.Send(ctx, dstAddr, coins)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// Create a new Discord session using the provided bot token.
	dg, err := discordgo.New("Bot " + botToken)
	if err != nil {
		log.Fatal(err)
	}

	// Register the messageCreate func as a callback for MessageCreate events.
	dg.AddHandler(messageCreate)

	// In this example, we only care about receiving message events.
	dg.Identify.Intents = discordgo.IntentsGuildMessages

	// Open a websocket connection to Discord and begin listening.
	err = dg.Open()
	if err != nil {
		log.Fatal(err)
	}

	// Wait here until CTRL-C or other term signal is received.
	log.Info("Bot is now running.  Press CTRL-C to exit.")
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
	<-sc

	// Cleanly close down the Discord session.
	dg.Close()
}

// This function will be called (due to AddHandler above) every time a new
// message is created on any channel that the authenticated bot has access to.
func messageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {

	// Ignore all messages created by the bot itself
	// This isn't required in this specific example but it's a good practice.
	if m.Author.ID == s.State.User.ID {
		return
	}
	// If the message is "ping" reply with "Pong!"
	if m.Content == "!ping" {
		err := s.MessageReactionAdd(m.ChannelID, m.ID, "👍")
		if err != nil {
			log.Error(err)
			return
		}
		err = s.MessageReactionAdd(m.ChannelID, m.ID, "💸")
		if err != nil {
			log.Error(err)
			return
		}
		_, err = s.ChannelMessageSend(m.ChannelID, "Pong!")
		if err != nil {
			log.Error(err)
			return
		}
	}

	// Do we support this command?
	re, err := regexp.Compile("!(request|help)(.*)")
	if err != nil {
		log.Error(err)
		help(s, m.ChannelID)
		return
	}

	// Do we support this bech32 prefix?
	matches := re.FindAllStringSubmatch(m.Content, -1)
	log.Printf("%#v\n", matches)
	if err == nil && len(matches) > 0 {
		cmd := strings.TrimSpace(matches[0][1])
		args := strings.TrimSpace(matches[0][2])
		switch cmd {
		case "request":
			dstAddr := args
			// TODO: don't hardcode, infer from address
			prefix := "umee"

			coins := cosmostypes.NewCoins(cosmostypes.NewCoins(
				cosmostypes.NewCoin("uumee", cosmostypes.NewInt(100000000)),
			)...)

			chain := chains.FindByPrefix(prefix)
			if chain == nil {
				log.Fatalf("%s prefix is not supported", prefix)
			}

			err = chain.Send(context.Background(), dstAddr, coins)
			if err != nil {
				log.Fatal(err)
			}

			err := s.MessageReactionAdd(m.ChannelID, m.ID, "👍")
			if err != nil {
				log.Error(err)
				return
			}
			err = s.MessageReactionAdd(m.ChannelID, m.ID, "💸")
			if err != nil {
				log.Error(err)
				return
			}
			_, err = s.ChannelMessageSend(m.ChannelID, fmt.Sprintf("Your address is `%s`; sent coins: %v.", dstAddr, coins))
			if err != nil {
				log.Error(err)
			}

		default:
			help(s, m.ChannelID)
		}
	} else {
		debugError(s, m.ChannelID, err)
	}
}

func debugError(s *discordgo.Session, channelID string, err error) {
	if err != nil {
		log.Error(err)
		_, err = s.ChannelMessageSend(channelID, fmt.Sprintf("Error:\n `%s`", err))
		if err != nil {
			log.Error(err)
		}
	}
}

//go:embed help.md
var helpMsg string

func help(s *discordgo.Session, channelID string) error {
	_, err := s.ChannelMessageSend(channelID, helpMsg)
	return err
}
