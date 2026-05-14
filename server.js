const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static(path.join(__dirname, 'public')));

// ─── Game State ────────────────────────────────────────────────────────────
const SUITS = ['♠', '♥', '♦', '♣'];
const RANKS = ['2','3','4','5','6','7','8','9','10','J','Q','K','A'];
const RANK_VAL = Object.fromEntries(RANKS.map((r,i) => [r, i+2]));

function makeDeck() {
  const d = [];
  for (const s of SUITS) for (const r of RANKS) d.push({ s, r });
  return d;
}
function shuffle(d) {
  for (let i = d.length-1; i > 0; i--) {
    const j = Math.floor(Math.random()*(i+1));
    [d[i], d[j]] = [d[j], d[i]];
  }
  return d;
}

// ─── Hand Evaluation ───────────────────────────────────────────────────────
function cardVal(c) { return RANK_VAL[c.r]; }
function evalHand(cards) {
  // Best 5 from 7
  const combos = combinations(cards, 5);
  let best = null;
  for (const combo of combos) {
    const score = score5(combo);
    if (!best || compareScore(score, best.score) > 0) best = { score, cards: combo };
  }
  return best;
}

function combinations(arr, k) {
  if (k === 0) return [[]];
  if (arr.length === 0) return [];
  const [first, ...rest] = arr;
  return [...combinations(rest, k-1).map(c => [first,...c]), ...combinations(rest, k)];
}

function score5(cards) {
  const vals = cards.map(cardVal).sort((a,b) => b-a);
  const suits = cards.map(c => c.s);
  const flush = suits.every(s => s === suits[0]);
  const straight = checkStraight(vals);
  const counts = {};
  for (const v of vals) counts[v] = (counts[v]||0)+1;
  const groups = Object.entries(counts).sort((a,b) => b[1]-a[1] || b[0]-a[0]);
  const groupCounts = groups.map(g => parseInt(g[1]));
  const groupVals = groups.map(g => parseInt(g[0]));

  if (flush && straight && vals[0] === 14) return [8, ...vals]; // Royal flush
  if (flush && straight) return [7, straight, ...vals]; // Straight flush
  if (groupCounts[0] === 4) return [6, groupVals[0], groupVals[1]]; // Four of a kind
  if (groupCounts[0] === 3 && groupCounts[1] === 2) return [5, groupVals[0], groupVals[1]]; // Full house
  if (flush) return [4, ...vals]; // Flush
  if (straight) return [3, straight, ...vals]; // Straight
  if (groupCounts[0] === 3) return [2, groupVals[0], ...groupVals.slice(1)]; // Three of a kind
  if (groupCounts[0] === 2 && groupCounts[1] === 2) return [1, Math.max(groupVals[0],groupVals[1]), Math.min(groupVals[0],groupVals[1]), groupVals[2]]; // Two pair
  if (groupCounts[0] === 2) return [0.5, groupVals[0], ...groupVals.slice(1)]; // One pair
  return [0, ...vals]; // High card
}

function checkStraight(vals) {
  const unique = [...new Set(vals)].sort((a,b) => b-a);
  for (let i = 0; i <= unique.length-5; i++) {
    if (unique[i] - unique[i+4] === 4 && new Set(unique.slice(i,i+5)).size === 5) return unique[i];
  }
  // Wheel A-2-3-4-5
  if (unique.includes(14) && unique.includes(2) && unique.includes(3) && unique.includes(4) && unique.includes(5)) return 5;
  return null;
}

function compareScore(a, b) {
  for (let i = 0; i < Math.max(a.length, b.length); i++) {
    const diff = (a[i]||0) - (b[i]||0);
    if (diff !== 0) return diff;
  }
  return 0;
}

const HAND_NAMES = [
  '高牌', '一对', '两对', '三条', '顺子', '同花', '葫芦', '四条', '同花顺', '皇家同花顺'
];
function handName(score) {
  const rank = score[0];
  if (rank === 8) return '皇家同花顺';
  if (rank === 7) return '同花顺';
  if (rank === 6) return '四条';
  if (rank === 5) return '葫芦';
  if (rank === 4) return '同花';
  if (rank === 3) return '顺子';
  if (rank === 2) return '三条';
  if (rank === 1) return '两对';
  if (rank === 0.5) return '一对';
  return '高牌';
}

// ─── Room Management ───────────────────────────────────────────────────────
const rooms = {}; // roomId -> Room

class Room {
  constructor(id) {
    this.id = id;
    this.players = []; // { ws, id, name, chips, hand, bet, folded, allIn, active }
    this.spectators = [];
    this.deck = [];
    this.community = [];
    this.pot = 0;
    this.sidePots = [];
    this.phase = 'waiting'; // waiting, preflop, flop, turn, river, showdown
    this.currentBet = 0;
    this.dealerIdx = 0;
    this.currentIdx = 0;
    this.smallBlind = 10;
    this.bigBlind = 20;
    this.minPlayers = 2;
    this.lastRaiseIdx = -1;
    this.actionCount = 0;
    this.roundStartIdx = 0;
  }

  broadcast(msg, exclude = null) {
    const data = JSON.stringify(msg);
    for (const p of this.players) {
      if (p.ws !== exclude && p.ws.readyState === WebSocket.OPEN) p.ws.send(data);
    }
    for (const s of this.spectators) {
      if (s.ws !== exclude && s.ws.readyState === WebSocket.OPEN) s.ws.send(data);
    }
  }

  sendState(target = null) {
    const recipients = target ? [target] : [...this.players, ...this.spectators];
    for (const p of recipients) {
      const isPlayer = this.players.includes(p);
      const state = this.getStateFor(p, isPlayer);
      if (p.ws.readyState === WebSocket.OPEN) {
        p.ws.send(JSON.stringify({ type: 'state', state }));
      }
    }
  }

  getStateFor(viewer, isPlayer) {
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
      players: this.players.map((p, i) => ({
        id: p.id,
        name: p.name,
        chips: p.chips,
        bet: p.bet,
        folded: p.folded,
        allIn: p.allIn,
        isYou: p.id === viewer.id,
        hand: (p.id === viewer.id || this.phase === 'showdown') ? p.hand : (p.hand ? ['??','??'] : null),
        active: p.active,
        disconnected: p.disconnected,
      })),
      myId: viewer.id,
      isMyTurn: isPlayer && this.players[this.currentIdx]?.id === viewer.id && this.phase !== 'waiting' && this.phase !== 'showdown',
      minRaise: this.currentBet * 2 || this.bigBlind,
    };
  }

  addPlayer(ws, id, name) {
    if (this.players.length >= 9) {
      this.spectators.push({ ws, id, name });
      this.sendState({ ws, id, name });
      return false;
    }
    const player = { ws, id, name, chips: 1000, hand: null, bet: 0, folded: false, allIn: false, active: false, disconnected: false };
    this.players.push(player);
    this.broadcast({ type: 'chat', msg: `${name} 加入了房间`, system: true });
    this.sendState();
    return true;
  }

  removePlayer(id) {
    const idx = this.players.findIndex(p => p.id === id);
    if (idx !== -1) {
      const p = this.players[idx];
      this.broadcast({ type: 'chat', msg: `${p.name} 离开了房间`, system: true });
      if (this.phase !== 'waiting') {
        p.disconnected = true;
        p.folded = true;
        this.checkAdvance();
      } else {
        this.players.splice(idx, 1);
      }
      this.sendState();
    }
  }

  startGame() {
    if (this.players.filter(p => !p.disconnected).length < this.minPlayers) return;
    // Remove disconnected
    this.players = this.players.filter(p => !p.disconnected);
    this.deck = shuffle(makeDeck());
    this.community = [];
    this.pot = 0;
    this.currentBet = 0;
    this.phase = 'preflop';
    
    for (const p of this.players) {
      p.hand = [this.deck.pop(), this.deck.pop()];
      p.bet = 0;
      p.folded = false;
      p.allIn = false;
      p.active = true;
    }

    // Blinds
    const n = this.players.length;
    const sbIdx = (this.dealerIdx + 1) % n;
    const bbIdx = (this.dealerIdx + 2) % n;
    
    this.placeBet(sbIdx, this.smallBlind, true);
    this.placeBet(bbIdx, this.bigBlind, true);
    this.currentBet = this.bigBlind;
    this.lastRaiseIdx = bbIdx;
    this.currentIdx = (bbIdx + 1) % n;
    this.roundStartIdx = this.currentIdx;

    this.broadcast({ type: 'chat', msg: `新一局开始！底注 ${this.smallBlind}/${this.bigBlind}`, system: true });
    this.sendState();
  }

  placeBet(idx, amount, blind = false) {
    const p = this.players[idx];
    const actual = Math.min(amount, p.chips);
    p.chips -= actual;
    p.bet += actual;
    this.pot += actual;
    if (p.chips === 0) p.allIn = true;
    return actual;
  }

  getPlayer(id) { return this.players.find(p => p.id === id); }

  handleAction(id, action, amount) {
    const p = this.getPlayer(id);
    if (!p || this.players[this.currentIdx]?.id !== id) return;
    if (this.phase === 'waiting' || this.phase === 'showdown') return;

    const idx = this.players.indexOf(p);

    if (action === 'fold') {
      p.folded = true;
      this.broadcast({ type: 'chat', msg: `${p.name} 弃牌`, system: true });
    } else if (action === 'check') {
      if (p.bet < this.currentBet) return; // Can't check if need to call
      this.broadcast({ type: 'chat', msg: `${p.name} 过牌`, system: true });
    } else if (action === 'call') {
      const toCall = this.currentBet - p.bet;
      const actual = this.placeBet(idx, toCall);
      this.broadcast({ type: 'chat', msg: `${p.name} 跟注 ${actual}`, system: true });
    } else if (action === 'raise') {
      const toCall = this.currentBet - p.bet;
      const raiseAmount = Math.max(amount, this.currentBet + this.bigBlind);
      const total = raiseAmount - p.bet;
      this.placeBet(idx, total);
      this.currentBet = raiseAmount;
      this.lastRaiseIdx = idx;
      this.broadcast({ type: 'chat', msg: `${p.name} 加注到 ${raiseAmount}`, system: true });
    } else if (action === 'allin') {
      const all = p.chips;
      const newBet = p.bet + all;
      if (newBet > this.currentBet) {
        this.currentBet = newBet;
        this.lastRaiseIdx = idx;
      }
      this.placeBet(idx, all);
      this.broadcast({ type: 'chat', msg: `${p.name} 全押 ${p.bet}！`, system: true });
    }

    this.checkAdvance();
    this.sendState();
  }

  completeAllInHand() {
    // 所有玩家都全押或弃牌，直接发完所有公共牌并摊牌
    const notFolded = this.players.filter(p => !p.folded);
    if (notFolded.length <= 1) return;

    // 发完所有公共牌
    while (this.community.length < 5 && this.deck.length > 0) {
      this.community.push(this.deck.pop());
    }

    this.phase = 'showdown';
    this.sendState();
    this.resolveShowdown();
  }

  checkAdvance() {
    const active = this.players.filter(p => !p.folded && !p.allIn);
    const notFolded = this.players.filter(p => !p.folded);

    // Only one left
    if (notFolded.length === 1) {
      const winner = notFolded[0];
      winner.chips += this.pot;
      this.broadcast({ type: 'chat', msg: `${winner.name} 赢得了底池 ${this.pot} 筹码！`, system: true });
      this.phase = 'showdown';
      setTimeout(() => this.nextRound(), 3000);
      return;
    }

    // 检查是否所有未弃牌玩家都已全押
    const allInAll = notFolded.every(p => p.allIn);
    if (allInAll && notFolded.length >= 2) {
      this.completeAllInHand();
      return;
    }

    // Check if betting round is over
    const needAction = active.filter(p => p.bet < this.currentBet);
    const allActed = this.allPlayersActed();

    if (active.length === 0 || (needAction.length === 0 && allActed)) {
      this.nextPhase();
      return;
    }

    // Advance to next player
    let next = (this.currentIdx + 1) % this.players.length;
    let tries = 0;
    while ((this.players[next].folded || this.players[next].allIn) && tries < this.players.length) {
      next = (next + 1) % this.players.length;
      tries++;
    }
    this.currentIdx = next;
    this.actionCount++;
  }

  allPlayersActed() {
    const active = this.players.filter(p => !p.folded && !p.allIn);
    if (active.length === 0) return true;
    for (const p of active) {
      if (p.bet < this.currentBet) return false;
    }
    // 检查是否从上次加注后已经轮了一圈
    if (this.lastRaiseIdx === -1) {
      // 没有加注，检查是否所有active玩家都已行动
      return true;
    }
    return true;
  }

  nextPhase() {
    // Reset bets for next round but keep pot
    for (const p of this.players) p.bet = 0;
    this.currentBet = 0;
    this.actionCount = 0;

    const phases = ['preflop', 'flop', 'turn', 'river', 'showdown'];
    const idx = phases.indexOf(this.phase);
    this.phase = phases[idx + 1] || 'showdown';

    if (this.phase === 'flop') {
      this.community.push(this.deck.pop(), this.deck.pop(), this.deck.pop());
    } else if (this.phase === 'turn' || this.phase === 'river') {
      this.community.push(this.deck.pop());
    } else if (this.phase === 'showdown') {
      this.resolveShowdown();
      return;
    }

    // Reset to first active after dealer
    const n = this.players.length;
    let start = (this.dealerIdx + 1) % n;
    while (this.players[start].folded || this.players[start].allIn) {
      start = (start + 1) % n;
    }
    this.currentIdx = start;
    this.lastRaiseIdx = -1;
    this.broadcast({ type: 'chat', msg: `--- ${this.phase.toUpperCase()} ---`, system: true });
  }

  resolveShowdown() {
    const contenders = this.players.filter(p => !p.folded);
    let bestScore = null, winners = [];

    for (const p of contenders) {
      const result = evalHand([...p.hand, ...this.community]);
      p.handResult = result;
      const cmp = bestScore ? compareScore(result.score, bestScore) : 1;
      if (cmp > 0) { bestScore = result.score; winners = [p]; }
      else if (cmp === 0) winners.push(p);
    }

    const share = Math.floor(this.pot / winners.length);
    for (const w of winners) w.chips += share;

    const handDesc = winners.map(w => `${w.name}(${handName(w.handResult.score)})`).join(', ');
    this.broadcast({ type: 'chat', msg: `🏆 赢家: ${handDesc}，赢得 ${this.pot} 筹码！`, system: true });

    this.phase = 'showdown';
    setTimeout(() => this.nextRound(), 4000);
  }

  nextRound() {
    this.players = this.players.filter(p => !p.disconnected && p.chips > 0);
    if (this.players.length < 2) {
      this.phase = 'waiting';
      this.broadcast({ type: 'chat', msg: '玩家不足，等待更多玩家加入...', system: true });
      this.sendState();
      return;
    }
    this.dealerIdx = (this.dealerIdx + 1) % this.players.length;
    this.startGame();
  }
}

// ─── WebSocket Handler ─────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  let playerId = null;
  let currentRoom = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      playerId = msg.id || Math.random().toString(36).slice(2);
      const roomId = msg.room || 'default';
      if (!rooms[roomId]) rooms[roomId] = new Room(roomId);
      currentRoom = rooms[roomId];
      currentRoom.addPlayer(ws, playerId, msg.name || '玩家');
    }

    if (!currentRoom || !playerId) return;

    if (msg.type === 'start') {
      if (currentRoom.phase === 'waiting') currentRoom.startGame();
    }

    if (msg.type === 'action') {
      currentRoom.handleAction(playerId, msg.action, msg.amount || 0);
    }

    if (msg.type === 'chat') {
      const p = currentRoom.getPlayer(playerId);
      const name = p ? p.name : '旁观者';
      currentRoom.broadcast({ type: 'chat', msg: `${name}: ${msg.text}`, system: false });
    }
  });

  ws.on('close', () => {
    if (currentRoom && playerId) currentRoom.removePlayer(playerId);
  });
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, '0.0.0.0', () => console.log(`🃏 德州扑克服务器运行在 http://localhost:${PORT}`));
