package kucoinapi

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
)

const NullPrice = "null"

type StreamTickerBranch struct {
	bid    tobBranch
	ask    tobBranch
	cancel *context.CancelFunc
	reCh   chan error
}

type tobBranch struct {
	mux   sync.RWMutex
	price string
	qty   string
}

type wS struct {
	Channel       string
	OnErr         bool
	Product       string
	Symbol        string
	Logger        *log.Logger
	Conn          *websocket.Conn
	LastUpdatedId decimal.Decimal
}

type subscribe struct {
	ID     int      `json:"id"`
	Method string   `json:"method"`
	Params []string `json:"params"`
}

// func SwapStreamTicker(symbol string, logger *log.Logger) *StreamTickerBranch {
// 	return localStreamTicker("swap", symbol, logger)
// }

// ex: symbol = btcusdt
func SpotStreamTicker(symbol string, logger *log.Logger) *StreamTickerBranch {
	return localStreamTicker("spot", symbol, logger)
}

func localStreamTicker(product, symbol string, logger *log.Logger) *StreamTickerBranch {
	var s StreamTickerBranch
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = &cancel
	ticker := make(chan map[string]interface{}, 50)
	errCh := make(chan error, 5)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := digifinexTickerSocket(ctx, product, symbol, "ticker", logger, &ticker, &errCh); err == nil {
					return
				} else {
					logger.Warningf("Reconnect %s %s ticker stream with err: %s\n", symbol, product, err.Error())
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := s.maintainStreamTicker(ctx, product, symbol, &ticker, &errCh); err == nil {
					return
				} else {
					logger.Warningf("Refreshing %s %s ticker stream with err: %s\n", symbol, product, err.Error())
				}
			}
		}
	}()
	return &s
}

func (s *StreamTickerBranch) Close() {
	(*s.cancel)()
	s.bid.mux.Lock()
	s.bid.price = NullPrice
	s.bid.mux.Unlock()
	s.ask.mux.Lock()
	s.ask.price = NullPrice
	s.ask.mux.Unlock()
}

func (s *StreamTickerBranch) GetBid() (price, qty string, ok bool) {
	s.bid.mux.RLock()
	defer s.bid.mux.RUnlock()
	price = s.bid.price
	qty = s.bid.qty
	if price == NullPrice || price == "" {
		return price, qty, false
	}
	return price, qty, true
}

func (s *StreamTickerBranch) GetAsk() (price, qty string, ok bool) {
	s.ask.mux.RLock()
	defer s.ask.mux.RUnlock()
	price = s.ask.price
	qty = s.ask.qty
	if price == NullPrice || price == "" {
		return price, qty, false
	}
	return price, qty, true
}

func (s *StreamTickerBranch) updateBidData(price, qty string) {
	s.bid.mux.Lock()
	defer s.bid.mux.Unlock()
	s.bid.price = price
	s.bid.qty = qty
}

func (s *StreamTickerBranch) updateAskData(price, qty string) {
	s.ask.mux.Lock()
	defer s.ask.mux.Unlock()
	s.ask.price = price
	s.ask.qty = qty
}

func (s *StreamTickerBranch) maintainStreamTicker(
	ctx context.Context,
	product, symbol string,
	ticker *chan map[string]interface{},
	errCh *chan error,
) error {
	lastUpdate := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		case message := <-(*ticker):
			var bidPrice, askPrice, bidQty, askQty string
			if bid, ok := message["best_bid"].(string); ok {
				bidPrice = bid
			} else {
				bidPrice = NullPrice
			}
			if ask, ok := message["best_ask"].(string); ok {
				askPrice = ask
			} else {
				askPrice = NullPrice
			}
			if bidqty, ok := message["best_bid_size"].(string); ok {
				bidQty = bidqty
			}
			if askqty, ok := message["best_ask_size"].(string); ok {
				askQty = askqty
			}
			s.updateBidData(bidPrice, bidQty)
			s.updateAskData(askPrice, askQty)
			lastUpdate = time.Now()
		default:
			if time.Now().After(lastUpdate.Add(time.Second * 60)) {
				// 60 sec without updating
				err := errors.New("reconnect because of time out")
				*errCh <- err
				return err
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func digifinexTickerSocket(
	ctx context.Context,
	product, symbol, channel string,
	logger *log.Logger,
	mainCh *chan map[string]interface{},
	errCh *chan error,
) error {
	var w wS
	var duration time.Duration = 30
	innerErr := make(chan error, 1)
	w.Logger = logger
	w.OnErr = false
	symbol = strings.ToUpper(symbol)
	w.Symbol = symbol
	w.Product = product
	url := "wss://openapi.digifinex.com/ws/v1/"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	logger.Infof("Digifinex %s %s %s socket connected.\n", symbol, product, channel)
	w.Conn = conn
	defer conn.Close()
	err = w.subscribeToTicker(symbol)
	if err != nil {
		return err
	}
	if err := w.Conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
		return err
	}
	w.Conn.SetPingHandler(nil)
	go func() {
		PingManaging := time.NewTicker(time.Second * 30)
		defer PingManaging.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-innerErr:
				return
			case <-PingManaging.C:
				w.sendPingPong()
			default:
				time.Sleep(time.Second)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-*errCh:
			return err
		default:
			_, buf, err := conn.ReadMessage()
			if err != nil {
				d := w.outDigifinexErr()
				*mainCh <- d
				innerErr <- errors.New("restart")
				return err
			}
			res, err1 := decodingMap(buf, logger)
			if err1 != nil {
				d := w.outDigifinexErr()
				*mainCh <- d
				innerErr <- errors.New("restart")
				return err
			}
			err2 := w.handleDigifinexSocketData(res, mainCh)
			if err2 != nil {
				d := w.outDigifinexErr()
				*mainCh <- d
				innerErr <- errors.New("restart")
				return err2
			}
			if err := w.Conn.SetReadDeadline(time.Now().Add(time.Second * duration)); err != nil {
				return err
			}
		}
	}
}

func decodingMap(message []byte, logger *log.Logger) (res map[string]interface{}, err error) {
	// decode by zlib
	buffer := bytes.NewBuffer(message)
	reader, err0 := zlib.NewReader(buffer)
	if err0 != nil {

	}
	defer reader.Close()
	content, err1 := ioutil.ReadAll(reader)
	if err1 != nil {

	}
	err = json.Unmarshal(content, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (w *wS) subscribeToTicker(symbol string) error {
	sub := subscribe{
		ID:     5243,
		Method: "ticker.subscribe",
		Params: []string{symbol},
	}
	message, err := json.Marshal(sub)
	if err != nil {
		return err
	}
	if err := w.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
		return err
	}
	return nil
}

func (w *wS) outDigifinexErr() map[string]interface{} {
	w.OnErr = true
	m := make(map[string]interface{})
	return m
}

func formatingTimeStamp(timeFloat float64) time.Time {
	t := time.Unix(int64(timeFloat/1000), 0)
	return t
}

func (w *wS) handleDigifinexSocketData(res map[string]interface{}, mainCh *chan map[string]interface{}) error {

	switch result := res["result"].(type) {
	case map[string]interface{}:
		// subscribed
		status := result["status"].(string)
		if strings.EqualFold(status, "success") {
			w.Logger.Infof("Subscribed to Digifinex %s %s.", w.Symbol, w.Product)
		} else {
			w.Logger.Warningf("Trying to subscribe Digifinex %s %s, but got status: %s", w.Symbol, w.Product, status)
		}
	case string:
		// ping pong
		if result == "pong" {
			w.Conn.SetReadDeadline(time.Now().Add(time.Second * 35))
		}
	default:
		// update ticker
		method, ok := res["method"].(string)
		if !ok {
			return errors.New("get nil method from an event")
		}
		switch method {
		case "ticker.update":
			params, ok := res["params"].([]interface{})
			if !ok {
				return errors.New("got nil when pharsing params")
			}
			for _, param := range params {
				data := param.(map[string]interface{})
				if st, ok := data["timestamp"].(float64); !ok {
					m := w.outDigifinexErr()
					*mainCh <- m
					return errors.New("got nil when updating event time")
				} else {
					stamp := formatingTimeStamp(st)
					if time.Now().After(stamp.Add(time.Second * 5)) {
						m := w.outDigifinexErr()
						*mainCh <- m
						return errors.New("websocket data delay more than 5 sec")
					}
					*mainCh <- data
				}
			}
		}
	}
	return nil
}

func (w *wS) sendPingPong() error {
	ping := subscribe{
		ID:     5243,
		Method: "server.ping",
		Params: []string{},
	}
	message, err := json.Marshal(ping)
	if err != nil {
		return err
	}
	if err := w.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
		w.Conn.SetReadDeadline(time.Now().Add(time.Second))
		return err
	}
	return nil
}
