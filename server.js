const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const app = express();
const server = http.createServer(app);
const io = new Server(server);
app.use(express.static('public'));
const rooms = new Map();
const games = [
  {id:'number', name:'숫자 눈치게임', desc:'겹치지 않는 숫자를 제출하면 점수!', type:'number'},
  {id:'reaction', name:'반응속도', desc:'신호가 뜨면 가장 빠르게 눌러라!', type:'reaction'},
  {id:'quiz', name:'초스피드 퀴즈', desc:'모두에게 같은 문제! 먼저 정답을 제출해라.', type:'quiz'},
  {id:'balance', name:'밸런스 선택', desc:'A/B 중 하나를 골라 다수와 소수 선택을 노려라.', type:'balance'}
];
const quizList = [
  {q:'무지개 색은 일반적으로 몇 가지로 구분할까?',a:['5','6','7','8'],correct:2},
  {q:'1시간은 몇 분일까?',a:['30','45','60','90'],correct:2},
  {q:'대한민국의 수도는?',a:['부산','서울','대전','인천'],correct:1},
  {q:'삼각형의 변은 몇 개일까?',a:['2','3','4','5'],correct:1},
  {q:'물의 화학식은?',a:['CO2','O2','H2O','NaCl'],correct:2}
];
function code(){return crypto.randomBytes(3).toString('hex').toUpperCase();}
function safeRoom(r){return {code:r.code,hostId:r.hostId,phase:r.phase,round:r.round,totalRounds:r.totalRounds,game:r.game,players:[...r.players.values()].map(p=>({id:p.id,name:p.name,ready:p.ready,score:p.score})),submitted:Object.keys(r.answers).length};}
function broadcast(r){io.to(r.code).emit('room:update',safeRoom(r));}
function finishRound(r){if(r.phase!=='playing')return;r.phase='result';clearTimeout(r.timer);
  let result={game:r.game,round:r.round,details:{}};
  if(r.game.type==='number'){
    const vals=Object.entries(r.answers);const counts={};vals.forEach(([,v])=>counts[v]=(counts[v]||0)+1);
    vals.forEach(([pid,v])=>{const p=r.players.get(pid);if(p&&counts[v]===1)p.score+=2;});result.details=counts;
  } else if(r.game.type==='reaction'){
    const ranked=Object.entries(r.answers).sort((a,b)=>a[1]-b[1]);ranked.forEach(([pid,ms],i)=>{const p=r.players.get(pid);if(p)p.score+=Math.max(0,5-i);});result.details=Object.fromEntries(ranked.map(([id,ms],i)=>[id,{ms,points:Math.max(0,5-i)}]));
  } else if(r.game.type==='quiz'){
    const q=r.question;Object.entries(r.answers).forEach(([pid,choice])=>{if(choice===q.correct){const p=r.players.get(pid);if(p)p.score+=3;}});result.details={correct:q.correct};
  } else if(r.game.type==='balance'){
    const counts={A:0,B:0};Object.values(r.answers).forEach(v=>counts[v]++);Object.entries(r.answers).forEach(([pid,v])=>{const p=r.players.get(pid);if(p&&counts[v]===1)p.score+=4;else if(p&&counts[v]!==Object.values(r.answers).length)p.score+=1;});result.details=counts;
  }
  r.lastResult=result;io.to(r.code).emit('round:result',result);broadcast(r);
}
function beginRound(r){if(r.players.size<2){io.to(r.code).emit('notice','최소 2명이 필요해요.');return;}
 r.round++;r.phase='playing';r.answers={};r.game=games[Math.floor(Math.random()*games.length)];r.question=r.game.type==='quiz'?quizList[Math.floor(Math.random()*quizList.length)]:null;r.startedAt=Date.now();
 if(r.game.type==='reaction'){r.reactionAt=Date.now()+1800+Math.random()*3500;r.timer=setTimeout(()=>io.to(r.code).emit('reaction:go'),r.reactionAt-Date.now());}
 io.to(r.code).emit('game:start',{game:r.game,round:r.round,totalRounds:r.totalRounds,question:r.question});broadcast(r);
 r.timer=setTimeout(()=>finishRound(r),r.game.type==='reaction'?10000:25000);
}
function startNext(r){if(r.round>=r.totalRounds){r.phase='final';io.to(r.code).emit('game:final', [...r.players.values()].sort((a,b)=>b.score-a.score).map(p=>({name:p.name,score:p.score})));broadcast(r);return;}
 r.players.forEach(p=>p.ready=false);beginRound(r);}
io.on('connection',socket=>{
 socket.on('room:create',({name})=>{let c;do{c=code();}while(rooms.has(c));const room={code:c,hostId:socket.id,phase:'lobby',round:0,totalRounds:5,players:new Map(),game:null,answers:{},timer:null};room.players.set(socket.id,{id:socket.id,name:String(name||'플레이어').slice(0,14),ready:false,score:0});rooms.set(c,room);socket.join(c);socket.emit('room:joined',{code:c,room:safeRoom(room)});});
 socket.on('room:join',({code,name})=>{const r=rooms.get(String(code||'').toUpperCase());if(!r){socket.emit('notice','방 코드를 찾을 수 없어요.');return;}if(r.phase!=='lobby'){socket.emit('notice','이미 게임이 시작된 방이에요.');return;}if(r.players.size>=6){socket.emit('notice','방이 꽉 찼어요.');return;}r.players.set(socket.id,{id:socket.id,name:String(name||'플레이어').slice(0,14),ready:false,score:0});socket.join(r.code);socket.emit('room:joined',{code:r.code,room:safeRoom(r)});broadcast(r);});
 socket.on('player:ready',()=>{const r=[...rooms.values()].find(x=>x.players.has(socket.id));if(!r||r.phase!=='lobby')return;r.players.get(socket.id).ready=true;broadcast(r);});
 socket.on('host:start',()=>{const r=[...rooms.values()].find(x=>x.players.has(socket.id));if(!r||r.hostId!==socket.id||r.phase!=='lobby')return;if(r.players.size<2){socket.emit('notice','친구가 한 명 이상 들어와야 시작할 수 있어요.');return;}r.players.forEach(p=>p.ready=true);startNext(r);});
 socket.on('game:answer',value=>{const r=[...rooms.values()].find(x=>x.players.has(socket.id));if(!r||r.phase!=='playing'||socket.id in r.answers)return;
 if(r.game.type==='number'){const n=Number(value);if(!Number.isInteger(n)||n<1||n>10)return;r.answers[socket.id]=n;}
 else if(r.game.type==='reaction'){if(Date.now()<r.reactionAt){socket.emit('notice','너무 빨라요! 이번 라운드 점수 0점.');r.answers[socket.id]=99999;}else r.answers[socket.id]=Date.now()-r.reactionAt;}
 else if(r.game.type==='quiz'){const n=Number(value);if(!Number.isInteger(n)||n<0||n>3)return;r.answers[socket.id]=n;}
 else if(r.game.type==='balance'){if(value!=='A'&&value!=='B')return;r.answers[socket.id]=value;}
 broadcast(r);if(Object.keys(r.answers).length>=r.players.size)finishRound(r);
 });
 socket.on('host:next',()=>{const r=[...rooms.values()].find(x=>x.players.has(socket.id));if(!r||r.hostId!==socket.id||r.phase!=='result')return;startNext(r);});
 socket.on('disconnect',()=>{for(const r of rooms.values()){if(!r.players.has(socket.id))continue;r.players.delete(socket.id);delete r.answers[socket.id];if(!r.players.size){clearTimeout(r.timer);rooms.delete(r.code);}else{if(r.hostId===socket.id)r.hostId=[...r.players.keys()][0];broadcast(r);}}});
});
const port=process.env.PORT||3000;server.listen(port,'0.0.0.0',()=>console.log(`Party server listening on ${port}`));
