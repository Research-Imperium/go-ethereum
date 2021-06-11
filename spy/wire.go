package spy

import (
	"encoding/hex"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"time"
)

/**
ETH wire protocol(https://github.com/ethereum/devp2p/blob/master/caps/eth.md)
EIP-2481 with request ID(https://eips.ethereum.org/EIPS/eip-2481)

0x01 NewBlockHashesMsg                                NewBlockHashesPacket              backend  [[blockhash₁: B_32, number₁: P], [blockhash₂: B_32, number₂: P], ...]
0x02 TransactionsMsg                                  TransactionsPacket                backend  [tx₁, tx₂, ...]
0x03 GetBlockHeadersMsg (Never Receive)               GetBlockHeadersPacket             peer     [request-id: P, [startblock: {P, B_32}, limit: P, skip: P, reverse: {0, 1}]]
0x04 BlockHeadersMsg                                  BlockHeadersPacket                backend  [request-id: P, [header₁, header₂, ...]]
0x05 GetBlockBodiesMsg (Never Receive)                GetBlockBodiesPacket              peer     [request-id: P, [blockhash₁: B_32, blockhash₂: B_32, ...]]
0x06 BlockBodiesMsg                                   BlockBodiesPacket                 backend  [request-id: P, [block-body₁, block-body₂, ...]]
0x07 NewBlockMsg                                      NewBlockPacket                    backend  [block, td: P]
0x08 NewPooledTransactionHashesMsg - ETH65            NewPooledTransactionHashesPacket  backend  [txhash₁: B_32, txhash₂: B_32, ...]
0x09 GetPooledTransactionsMsg - ETH65 (Never Receive) GetPooledTransactionsPacket       peer     [request-id: P, [txhash₁: B_32, txhash₂: B_32, ...]]
0x0a PooledTransactionsMsg - ETH65                    PooledTransactionsPacket          backend  [request-id: P, [tx₁, tx₂...]]
0x0d GetNodeDataMsg                                   GetNodeDataPacket                 peer     [request-id: P, [hash₁: B_32, hash₂: B_32, ...]]
0x0e NodeDataMsg                                      NodeDataPacket                    backend  [request-id: P, [value₁: B, value₂: B, ...]]
0x0f GetReceiptsMsg                                   GetReceiptsPacket                 peer     [request-id: P, [blockhash₁: B_32, blockhash₂: B_32, ...]]
0x10 ReceiptsMsg                                      ReceiptsPacket                    backend  [request-id: P, [[receipt₁, receipt₂], ...]]
*/

type Wire0x01Msg struct {
	ID          uint `gorm:"primarykey"`
	PeerID      string
	ReceivedAt  time.Time
	BlockHash   string
	BlockNumber uint
}

type Wire0x02Msg struct {
	ID         uint `gorm:"primarykey"`
	PeerID     string
	ReceivedAt time.Time
	TxHash     string
}

type Wire0x07Msg struct {
	ID          uint `gorm:"primarykey"`
	PeerID      string
	ReceivedAt  time.Time
	BlockHash   string
	BlockNumber uint
}

type Wire0x08Msg struct {
	ID         uint `gorm:"primarykey"`
	PeerID     string
	ReceivedAt time.Time
	TxHash     string
}

type Transaction struct {
	Hash     string `gorm:"primaryKey"`
	To       string
	From     string
	Nonce    uint
	Value    string
	GasPrice string
	Gas      uint
	Data     string
	V        string
	R        string
	S        string
}

type WireSpy struct {
	Channel0x01 chan *Wire0x01Msg
	Channel0x02 chan *Wire0x02Msg
	Channel0x07 chan *Wire0x07Msg
	Channel0x08 chan *Wire0x08Msg
	ChannelTransaction      chan *types.Transaction
	// signer used to translate tx to message
	signer types.Signer
}

//type Transaction struct {
//
//}
//
//type WireBlock struct {
//
//}

func NewWireSpy(channelSize int) *WireSpy {
	spy := WireSpy{
		Channel0x01:             make(chan *Wire0x01Msg, channelSize),
		Channel0x02:             make(chan *Wire0x02Msg, channelSize),
		Channel0x07:             make(chan *Wire0x07Msg, channelSize),
		Channel0x08:             make(chan *Wire0x08Msg, channelSize),
		ChannelTransaction:      make(chan *types.Transaction, channelSize),

		signer: types.LatestSigner(params.MainnetChainConfig),
	}
	go spy.execute()
	return &spy
}

func (w *WireSpy) Close() {
	close(w.Channel0x01)
	close(w.Channel0x02)
	close(w.Channel0x07)
	close(w.Channel0x08)
	close(w.ChannelTransaction)
}

func (w *WireSpy) execute() {
	log.Info("Starting WireSpy Node")

	dsn := "host=localhost user=postgres password=password dbname=postgres port=5432 sslmode=disable"
	db, err := gorm.Open(
		postgres.Open(dsn),
		&gorm.Config{
			NamingStrategy: schema.NamingStrategy{TablePrefix: "ethereum_"},
		})

	if err != nil {
		panic("failed to connect database")
	}

	// Table auto migration
	if db.AutoMigrate(&Wire0x01Msg{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&Wire0x02Msg{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&Wire0x07Msg{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&Wire0x08Msg{}) != nil {
		panic("Failed to migrate db")
	}
	if db.AutoMigrate(&Transaction{}) != nil {
		panic("Failed to migrate db")
	}

	log.Info("SPY - Wire postgres connected")

	//TODO: We can use a cache to optimise the code below, but lets keep it simple for now:
	for {
		select {
		case obj := <-w.Channel0x01:
			db.Create(&obj)
		case obj := <-w.Channel0x02:
			db.Create(&obj)
		case obj := <-w.Channel0x07:
			db.Create(&obj)
		case obj := <-w.Channel0x08:
			db.Create(&obj)
		case obj := <-w.ChannelTransaction:

			var toAddress string
			if obj.To() == nil {
				toAddress = ""
			} else {
				toAddress = obj.To().Hex()
			}

			msg, err := obj.AsMessage(w.signer)
			if err != nil {
				log.Error("SPY - Failed to sign transaction", "hash", obj.Hash())
			}

			v, r, s := obj.RawSignatureValues()

			db.Clauses(clause.OnConflict{DoNothing: true}).Create(&Transaction{
				Hash:     obj.Hash().Hex(),
				To:       toAddress,
				From:     msg.From().Hex(),
				Nonce:    uint(msg.Nonce()),
				Value:    msg.Value().String(),
				GasPrice: msg.GasPrice().String(),
				Gas:      uint(msg.Gas()),
				Data:     hex.EncodeToString(msg.Data()),
				V:        v.String(),
				R:        r.String(),
				S:        s.String(),
			})
		default:
			continue
		}
	}
}
