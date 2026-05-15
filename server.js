const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static(path.join(__dirname, 'public')));

const SUITS = ['♠','♥','♦','♣'];
const RANKS = ['2','3','4','5','6','7','8','9','10','J','Q','K','A'];
const RV = Object.fromEntries(RANKS.map((r,i) => [r, i+2]));

function makeDeck() {
  const d = [];
  for (const s of SUITS) for (const r of RANKS) d.push({s,r});
  return d;
}
function shuffle(d) {
  for (let i = d.length-1; i > 0; i--) {
    const j = Math.floor(Math.random()*(i+1));
    [d[i],d[j]] = [d[j],d[i]];
  }
  return d;
}
function combos(arr, k) {
  if (k===0) return [[]];
  if (!arr.length) return [];
  const [h,...t] = arr;
  return [...combos(t,k-1).map(c=>[h,...c]),...combos(t,k)];
}
function score5(cards) {
  const vals = cards.map(c=>RV[c.r]).sort((a,b)=>b-a);
  const suits = cards.map(c=>c.s);
  const flush = suits.every(s=>s===suits[0]);
  const st = checkSt(vals);
  const cnt = {};
  for (const v of vals) cnt[v]=(cnt[v]||0)+1;
  const grps = Object.entries(cnt).sort((a,b)=>b[1]-a[1]||b[0]-a[0]);
  const gc = grps.map(g=>+g[1]), gv = grps.map(g=>+g[0]);
  if (flush&&st&&vals[0]===14) return [8,...vals];
  if (flush&&st) return [7,st,...vals];
  if (gc[0]===4) return [6,gv[0],gv[1]];
  if (gc[0]===3&&gc[1]===2) return [5,gv[0],gv[1]];
  if (flush) return [4,...vals];
  if (st) return [3,st,...vals];
  if (gc[0]===3) return [2,gv[0],...gv.slice(1)];
  if (gc[0]===2&&gc[1]===2) return [1,Math.max(gv[0],gv[1]),Math.min(gv[0],gv[1]),gv[2]];
  if (gc[0]===2) return [0.5,gv[0],...gv.slice(1)];
  return [0,...vals];
}
function checkSt(vals) {
  const u = [...new Set(vals)].sort((a,b)=>b-a);
  for (let i=0; i<=u.length-5; i++)
    if (u[i]-u[i+4]===4 && new Set(u.slice(i,i+5)).size===5) return u[i];
  if ([14,2,3,4,5].every(v=>u.includes(v))) return 5;
  return null;
}
function cmpScore(a,b) {
  for (let i=0; i<Math.max(a.length,b.length); i++) {
    const d=(a[i]||0)-(b[i]||0);
    if (d) return d;
  }
  return 0;
}
function evalHand(cards) {
  let best=null;
  for (const c of combos(cards,5)) {
    const s=score5(c);
    if (!best||cmpScore(s,best.score)>0) best={score:s,cards:c};
  }
  return best;
}
function handName(sc) {
  const r=sc[0];
  if (r===8) return '皇家同花顺';
  if (r===7) return '同花顺';
  if (r===6) return '四条';
  if (r===5) return '葫芦';
  if (r===4) return '同花';
  if (r===3) return '顺子';
  if (r===2) return '三条';
  if (r===1) return '两对';
  if (r===0.5) return '一对';
  return '高牌';
}

const rooms = {};

class Room {
  constructor(id) {
    this.id = id;
    this.players = [];
    this.deck = [];
    this.community = [];
    this.pot = 0;
    this.phase = 'waiting';
    this.currentBet = 0;
    this.dealerIdx = 0;
    this.currentIdx = 0;
    this.smallBlind = 10;
    this.bigBlind = 20;
    this.nextRoundTimer = null;
    this.actionTimer = null;
  }

  send(ws, msg) {
    try { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg)); } catch(e) {}
  }

  broadcast(msg, excludeId) {
    const data = JSON.stringify(msg);
    for (const p of this.players) {
      try {
        if (p.id !== excludeId && p.ws.readyState === WebSocket.OPEN) p.ws.send(data);
      } catch(e) {}
    }
  }

  sendState() {
    for (const p of this.players) {
      this.send(p.ws, { type: 'state', state: this.getStateFor(p) });
    }
  }

  getStateFor(viewer) {
    const isMyTurn = this.players[this.currentIdx]?.id === viewer.id
      && this.phase !== 'waiting' && this.phase !== 'showdown';
    return {
      roomId: this.id,
      phase: this.phase,
      pot: this.pot,
      community: this.community,
      currentBet: this.currentBet,
      smallBlind: this.smallBlind,
      bigBlind: this.bigBlind,
      dealerIdx: this.dealerIdx,
      currentPlayerIdx: this.currentIdx,
      myId: viewer.id,
      isMyTurn,
      players: this.players.map(p => ({
        id: p.id,
        name: p.name,
        chips: p.chips,
        bet: p.bet,
        folded: p.folded,
        allIn: p.allIn,
        disconnected: p.disconnected,
        hand: (p.id === viewer.id || this.phase === 'showdown') ? p.hand : (p.hand ? ['??','??'] : null),
        handResult: this.phase === 'showdown' && !p.folded && p.handResult
          ? { name: handName(p.handResult.score) } : null,
      })),
    };
  }

  addPlayer(ws, id, name) {
    const existing = this.players.find(p => p.id === id);
    if (existing) {
      existing.ws = ws;
      existing.disconnected = false;
      this.broadcast({ type: 'chat', msg: `${name} 重新连接`, system: true }, id);
      this.send(ws, { type: 'state', state: this.getStateFor(existing) });
      this.sendState();
      return;
    }
    if (this.players.length >= 9) {
      this.send(ws, { type: 'error', msg: '房间已满（最多9人）' });
      return;
    }
    const player = { ws, id, name, chips: 1000, hand: null, bet: 0, folded: false, allIn: false, disconnected: false, handResult: null };
    this.players.push(player);
    this.broadcast({ type: 'chat', msg: `${name} 加入了房间`, system: true });
    this.sendState();
  }

  removePlayer(id) {
    const p = this.players.find(p => p.id === id);
    if (!p) return;
    p.disconnected = true;
    this.broadcast({ type: 'chat', msg: `${p.name} 断开连接`, system: true });
    if (this.phase === 'waiting') {
      this.players = this.players.filter(x => x.id !== id);
    } else {
      p.folded = true;
      if (this.players[this.currentIdx]?.id === id) {
        this.advance();
      } else {
        this.checkWinner();
      }
    }
    this.sendState();
  }

  startGame() {
    this.players = this.players.filter(p => !p.disconnected && p.chips > 0);
    if (this.players.length < 2) return;
    clearTimeout(this.nextRoundTimer);
    clearTimeout(this.actionTimer);
    this.deck = shuffle(makeDeck());
    this.community = [];
    this.pot = 0;
    this.currentBet = 0;
    this.phase = 'preflop';
    for (const p of this.players) {
      p.hand = [this.deck.pop(), this.deck.pop()];
      p.bet = 0; p.folded = false; p.allIn = false; p.handResult = null;
    }
    const n = this.players.length;
    const sbIdx = (this.dealerIdx + 1) % n;
    const bbIdx = (this.dealerIdx + 2) % n;
    this.placeBet(sbIdx, this.smallBlind);
    this.placeBet(bbIdx, this.bigBlind);
    this.currentBet = this.bigBlind;
    this.currentIdx = (bbIdx + 1) % n;
    this.broadcast({ type: 'chat', msg: `新一局开始！盲注 ${this.smallBlind}/${this.bigBlind}`, system: true });
    this.sendState();
    this.setActionTimer();
  }

  placeBet(idx, amount) {
    const p = this.players[idx];
    const actual = Math.min(amount, p.chips);
    p.chips -= actual; p.bet += actual; this.pot += actual;
    if (p.chips === 0) p.allIn = true;
    return actual;
  }

  handleAction(id, action, amount) {
    const p = this.players[this.currentIdx];
    if (!p || p.id !== id) return;
    if (this.phase === 'waiting' || this.phase === 'showdown') return;
    clearTimeout(this.actionTimer);
    const idx = this.currentIdx;

    if (action === 'fold') {
      p.folded = true;
      this.broadcast({ type: 'chat', msg: `${p.name} 弃牌`, system: true });
    } else if (action === 'check') {
      if (p.bet < this.currentBet) return;
      this.broadcast({ type: 'chat', msg: `${p.name} 过牌`, system: true });
    } else if (action === 'call') {
      const actual = this.placeBet(idx, this.currentBet - p.bet);
      this.broadcast({ type: 'chat', msg: `${p.name} 跟注 ${actual}`, system: true });
    } else if (action === 'raise') {
      const ra = Math.max(amount, this.currentBet + this.bigBlind);
      const capped = Math.min(ra, p.chips + p.bet);
      this.placeBet(idx, capped - p.bet);
      this.currentBet = p.bet;
      this.broadcast({ type: 'chat', msg: `${p.name} 加注到 ${this.currentBet}`, system: true });
    } else if (action === 'allin') {
      const nb = p.bet + p.chips;
      if (nb > this.currentBet) this.currentBet = nb;
      this.placeBet(idx, p.chips);
      this.broadcast({ type: 'chat', msg: `${p.name} 全押！共 ${p.bet}`, system: true });
    }
    this.advance();
    this.sendState();
  }

  checkWinner() {
    const alive = this.players.filter(p => !p.folded);
    if (alive.length === 1) {
      alive[0].chips += this.pot;
      this.broadcast({ type: 'chat', msg: `🏆 ${alive[0].name} 赢得底池 ${this.pot}！`, system: true });
      this.phase = 'showdown';
      this.sendState();
      this.nextRoundTimer = setTimeout(() => this.nextRound(), 3500);
      return true;
    }
    return false;
  }

  advance() {
    if (this.checkWinner()) return;
    const active = this.players.filter(p => !p.folded && !p.allIn && !p.disconnected);
    const needCall = active.filter(p => p.bet < this.currentBet);
    if (active.length === 0 || (needCall.length === 0 && this.roundComplete())) {
      this.nextPhase();
      return;
    }
    let next = (this.currentIdx + 1) % this.players.length;
    let tries = 0;
    while (tries < this.players.length) {
      const np = this.players[next];
      if (!np.folded && !np.allIn && !np.disconnected) break;
      next = (next + 1) % this.players.length;
      tries++;
    }
    if (tries >= this.players.length) { this.nextPhase(); return; }
    this.currentIdx = next;
    this.setActionTimer();
  }

  roundComplete() {
    return this.players
      .filter(p => !p.folded && !p.allIn && !p.disconnected)
      .every(p => p.bet >= this.currentBet);
  }

  setActionTimer() {
    clearTimeout(this.actionTimer);
    this.actionTimer = setTimeout(() => {
      const p = this.players[this.currentIdx];
      if (p && !p.folded && !p.allIn) {
        this.broadcast({ type: 'chat', msg: `${p.name} 超时自动弃牌`, system: true });
        p.folded = true;
        this.advance();
        this.sendState();
      }
    }, 60000);
  }

  nextPhase() {
    for (const p of this.players) p.bet = 0;
    this.currentBet = 0;
    const phases = ['preflop','flop','turn','river','showdown'];
    const i = phases.indexOf(this.phase);
    this.phase = phases[i+1] || 'showdown';
    if (this.phase === 'flop') {
      this.community.push(this.deck.pop(), this.deck.pop(), this.deck.pop());
    } else if (this.phase === 'turn' || this.phase === 'river') {
      this.community.push(this.deck.pop());
    } else if (this.phase === 'showdown') {
      this.resolveShowdown(); return;
    }
    const n = this.players.length;
    let start = (this.dealerIdx + 1) % n, tries = 0;
    while (tries < n) {
      if (!this.players[start].folded && !this.players[start].allIn && !this.players[start].disconnected) break;
      start = (start + 1) % n; tries++;
    }
    this.currentIdx = tries < n ? start : 0;
    const names = { flop:'翻牌', turn:'转牌', river:'河牌' };
    this.broadcast({ type: 'chat', msg: `─── ${names[this.phase]} ───`, system: true });
    this.sendState();
    this.setActionTimer();
  }

  resolveShowdown() {
    const contenders = this.players.filter(p => !p.folded);
    let bestScore = null, winners = [];
    for (const p of contenders) {
      if (!p.hand || this.community.length < 3) continue;
      const res = evalHand([...p.hand, ...this.community]);
      p.handResult = res;
      const cmp = bestScore ? cmpScore(res.score, bestScore) : 1;
      if (cmp > 0) { bestScore = res.score; winners = [p]; }
      else if (cmp === 0) winners.push(p);
    }
    if (!winners.length) winners = contenders;
    const share = Math.floor(this.pot / winners.length);
    for (const w of winners) w.chips += share;
    const desc = winners.map(w => `${w.name}(${w.handResult ? handName(w.handResult.score) : '?'})`).join('、');
    this.broadcast({ type: 'chat', msg: `🏆 赢家：${desc}，赢得 ${this.pot}！`, system: true });
    this.phase = 'showdown';
    this.sendState();
    this.nextRoundTimer = setTimeout(() => this.nextRound(), 4000);
  }

  nextRound() {
    clearTimeout(this.actionTimer);
    this.players = this.players.filter(p => !p.disconnected && p.chips > 0);
    if (this.players.length < 2) {
      this.phase = 'waiting';
      this.broadcast({ type: 'chat', msg: '玩家不足，等待更多玩家加入…', system: true });
      this.sendState(); return;
    }
    this.dealerIdx = (this.dealerIdx + 1) % this.players.length;
    this.startGame();
  }
}

// Heartbeat
function heartbeat() { this.isAlive = true; }

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', heartbeat);
  let playerId = null, currentRoom = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'join') {
      playerId = msg.id || Math.random().toString(36).slice(2,10);
      const roomId = String(msg.room || 'default').slice(0,30);
      if (!rooms[roomId]) rooms[roomId] = new Room(roomId);
      currentRoom = rooms[roomId];
      currentRoom.addPlayer(ws, playerId, String(msg.name || '玩家').slice(0,14));
      return;
    }
    if (!currentRoom || !playerId) return;
    if (msg.type === 'start') {
      if (currentRoom.phase === 'waiting' && currentRoom.players.length >= 2)
        currentRoom.startGame();
    }
    if (msg.type === 'action') currentRoom.handleAction(playerId, msg.action, Number(msg.amount)||0);
    if (msg.type === 'chat') {
      const p = currentRoom.players.find(p => p.id === playerId);
      currentRoom.broadcast({ type: 'chat', msg: `${p ? p.name : '?'}: ${String(msg.text||'').slice(0,200)}`, system: false });
    }
    if (msg.type === 'ping') { try { ws.send(JSON.stringify({type:'pong'})); } catch(e) {} }
  });

  ws.on('close', () => { if (currentRoom && playerId) currentRoom.removePlayer(playerId); });
  ws.on('error', () => { if (currentRoom && playerId) currentRoom.removePlayer(playerId); });
});

const pingInterval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    try { ws.ping(); } catch(e) {}
  });
}, 25000);
wss.on('close', () => clearInterval(pingInterval));

setInterval(() => {
  for (const [id, room] of Object.entries(rooms)) {
    if (room.players.length === 0) delete rooms[id];
  }
}, 600000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🃏 德州扑克服务器运行在 http://localhost:${PORT}`);
});
