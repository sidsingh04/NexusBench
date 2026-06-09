package worker_test

import "sync"

type inMemoryOrderbook struct {
	mu    sync.Mutex
	buys  []bookEntry
	sells []bookEntry
	index map[string]int // id → index in buys/sells (simplified)
	seq   int            // per-instance sequence counter; no package-level state
}

type bookEntry struct {
	id         string
	side       string
	price      int64
	remainingQ int64
	seq        int
}

func newOrderbook() *inMemoryOrderbook {
	return &inMemoryOrderbook{index: make(map[string]int)}
}

func (ob *inMemoryOrderbook) apply(id, kind, side string, price, qty int64) orderFill {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	switch kind {
	case "cancel":
		return ob.applyCancel(id)
	case "limit":
		return ob.applyLimit(id, side, price, qty)
	case "market":
		return ob.applyMarket(id, side, qty)
	default:
		return orderFill{OrderID: id, Accepted: false}
	}
}

func (ob *inMemoryOrderbook) applyCancel(id string) orderFill {
	// Try to remove from buys
	for i, e := range ob.buys {
		if e.id == id {
			ob.buys = append(ob.buys[:i], ob.buys[i+1:]...)
			delete(ob.index, id)
			return orderFill{OrderID: id, Accepted: true}
		}
	}
	// Try to remove from sells
	for i, e := range ob.sells {
		if e.id == id {
			ob.sells = append(ob.sells[:i], ob.sells[i+1:]...)
			delete(ob.index, id)
			return orderFill{OrderID: id, Accepted: true}
		}
	}
	return orderFill{OrderID: id, Accepted: false}
}

func (ob *inMemoryOrderbook) applyLimit(id, side string, price, qty int64) orderFill {
	if qty <= 0 || price <= 0 {
		return orderFill{OrderID: id, Accepted: false}
	}
	ob.seq++
	remaining := qty
	var execPrice int64
	var execQty int64

	if side == "buy" {
		// Sort sells ascending by price, then by seq
		sortSells(ob.sells)
		i := 0
		for i < len(ob.sells) && remaining > 0 {
			s := &ob.sells[i]
			if s.price > price {
				break
			}
			match := min64(remaining, s.remainingQ)
			execPrice = s.price
			execQty += match
			remaining -= match
			s.remainingQ -= match
			if s.remainingQ == 0 {
				ob.sells = append(ob.sells[:i], ob.sells[i+1:]...)
			} else {
				i++
			}
		}
	} else {
		// Sort buys descending by price, then by seq
		sortBuys(ob.buys)
		i := 0
		for i < len(ob.buys) && remaining > 0 {
			b := &ob.buys[i]
			if b.price < price {
				break
			}
			match := min64(remaining, b.remainingQ)
			execPrice = b.price
			execQty += match
			remaining -= match
			b.remainingQ -= match
			if b.remainingQ == 0 {
				ob.buys = append(ob.buys[:i], ob.buys[i+1:]...)
			} else {
				i++
			}
		}
	}

	if remaining > 0 {
		entry := bookEntry{id: id, side: side, price: price, remainingQ: remaining, seq: ob.seq}
		if side == "buy" {
			ob.buys = append(ob.buys, entry)
		} else {
			ob.sells = append(ob.sells, entry)
		}
		ob.seq++
		ob.index[id] = ob.seq
	}

	return orderFill{OrderID: id, Accepted: true, ExecutedPrice: execPrice, ExecutedQty: execQty}
}

func (ob *inMemoryOrderbook) applyMarket(id, side string, qty int64) orderFill {
	if qty <= 0 {
		return orderFill{OrderID: id, Accepted: false}
	}
	remaining := qty
	var execPrice int64
	var execQty int64

	if side == "buy" {
		sortSells(ob.sells)
		i := 0
		for i < len(ob.sells) && remaining > 0 {
			s := &ob.sells[i]
			match := min64(remaining, s.remainingQ)
			execPrice = s.price
			execQty += match
			remaining -= match
			s.remainingQ -= match
			if s.remainingQ == 0 {
				ob.sells = append(ob.sells[:i], ob.sells[i+1:]...)
			} else {
				i++
			}
		}
	} else {
		sortBuys(ob.buys)
		i := 0
		for i < len(ob.buys) && remaining > 0 {
			b := &ob.buys[i]
			match := min64(remaining, b.remainingQ)
			execPrice = b.price
			execQty += match
			remaining -= match
			b.remainingQ -= match
			if b.remainingQ == 0 {
				ob.buys = append(ob.buys[:i], ob.buys[i+1:]...)
			} else {
				i++
			}
		}
	}

	if execQty == 0 {
		return orderFill{OrderID: id, Accepted: false}
	}
	return orderFill{OrderID: id, Accepted: true, ExecutedPrice: execPrice, ExecutedQty: execQty}
}

func sortBuys(buys []bookEntry) {
	for i := 1; i < len(buys); i++ {
		for j := i; j > 0; j-- {
			if buys[j].price > buys[j-1].price ||
				(buys[j].price == buys[j-1].price && buys[j].seq < buys[j-1].seq) {
				buys[j], buys[j-1] = buys[j-1], buys[j]
			} else {
				break
			}
		}
	}
}

func sortSells(sells []bookEntry) {
	for i := 1; i < len(sells); i++ {
		for j := i; j > 0; j-- {
			if sells[j].price < sells[j-1].price ||
				(sells[j].price == sells[j-1].price && sells[j].seq < sells[j-1].seq) {
				sells[j], sells[j-1] = sells[j-1], sells[j]
			} else {
				break
			}
		}
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// wrongPriceEngine returns a server that always returns ExecutedPrice=9999
// for filled orders (a deliberately wrong price).

type orderFill struct {
	OrderID       string
	Accepted      bool
	ExecutedPrice int64
	ExecutedQty   int64
}
