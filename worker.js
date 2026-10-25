// spacemate-push Worker: 10 percenként (cron) megnézi, kinek járt le a szükséglete, és üres Web Push-t küld.
// Kötések/titkok: DB (D1: theta-usage), VAPID_PUBLIC (változó), VAPID_D (TITOK), VAPID_SUB (változó, pl. mailto:te@példa.hu vagy https://spacemate.radvanszki.com)
const te=s=>new TextEncoder().encode(s);
const b64uBytes=b=>{const r=atob((b+'='.repeat((4-b.length%4)%4)).replace(/-/g,'+').replace(/_/g,'/'));return Uint8Array.from(r,c=>c.charCodeAt(0))};
const b64u=u=>btoa(String.fromCharCode(...new Uint8Array(u))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
const MISS_H=10,MAXN=3,GAP=90*6e4,OPENW=10*6e4,PER_RUN=20; // PER_RUN: az ingyenes csomag ~50 kérés/futás korlátja miatt
const HOSTS=[/(^|\.)push\.apple\.com$/,/^fcm\.googleapis\.com$/,/(^|\.)push\.services\.mozilla\.com$/,/(^|\.)notify\.windows\.com$/];
const okEp=ep=>{try{const u=new URL(ep);return u.protocol=='https:'&&HOSTS.some(r=>r.test(u.hostname))}catch{return false}};

const asleep=(t,tz)=>{const h=new Date(t-tz*6e4).getUTCHours();return h>=23||h<7};   // 23–7 helyi idő: nem küldünk
const wake=(t,tz)=>{let x=t;for(let i=0;i<60&&asleep(x,tz);i++)x+=18e5;return x};

// a legkorábbi időpont, amikor ennek a sornak szólnia kell (a szünetekkel és az időközzel együtt)
function planNxt(r,dues,sent){let m=Infinity;
  if(r.n<MAXN)for(const c in dues)for(const k in dues[c]){const d=dues[c][k];if(sent[c+':'+k]!==d&&d<m)m=d}
  if(!r.ms&&r.seen+MISS_H*36e5<m)m=r.seen+MISS_H*36e5;
  if(!isFinite(m))return 0;
  m=Math.max(m,r.lp?r.lp+GAP:0,r.seen+OPENW);
  return asleep(m,r.tz)?wake(m,r.tz):m}

// mit küldjünk most (vagy semmit)
function decide(r,now){
  let dues={},sent={};try{dues=JSON.parse(r.dues||'{}');sent=JSON.parse(r.sent||'{}')}catch{}
  const can=!asleep(now,r.tz)&&(!r.lp||now-r.lp>=GAP)&&now-r.seen>=OPENW;
  let cand=null;
  if(r.n<MAXN)for(const c in dues)for(const k in dues[c]){const d=dues[c][k];if(d<=now&&sent[c+':'+k]!==d&&(!cand||d<cand.d))cand={c,k,d}}
  if(can&&cand){const s2={...sent,[cand.c+':'+cand.k]:cand.d},r2={...r,n:r.n+1,lp:now};
    return {send:{c:cand.c,k:cand.k},upd:{n:r2.n,lp:now,sent:JSON.stringify(s2),ms:r.ms,msg:JSON.stringify({c:cand.c,k:cand.k}),nxt:planNxt(r2,dues,s2)}}}
  if(can&&!r.ms&&now-r.seen>=MISS_H*36e5){const c=r.last||'zizi',r2={...r,ms:1,lp:now};
    return {send:{c,k:'miss'},upd:{n:r.n,lp:now,sent:JSON.stringify(sent),ms:1,msg:JSON.stringify({c,k:'miss'}),nxt:planNxt(r2,dues,sent)}}}
  return {send:null,upd:{nxt:planNxt(r,dues,sent)}}}

async function vapidJwt(env,aud){
  const pub=b64uBytes(env.VAPID_PUBLIC);
  const key=await crypto.subtle.importKey('jwk',{kty:'EC',crv:'P-256',x:b64u(pub.slice(1,33)),y:b64u(pub.slice(33,65)),d:env.VAPID_D,ext:true},{name:'ECDSA',namedCurve:'P-256'},false,['sign']);
  const h=b64u(te(JSON.stringify({typ:'JWT',alg:'ES256'}))),p=b64u(te(JSON.stringify({aud,exp:Math.floor(Date.now()/1e3)+12*3600,sub:env.VAPID_SUB||'https://spacemate.radvanszki.com'})));
  const sig=await crypto.subtle.sign({name:'ECDSA',hash:'SHA-256'},key,te(h+'.'+p));
  return h+'.'+p+'.'+b64u(sig)}

async function sendPush(env,ep){
  if(!okEp(ep))return {gone:true};
  try{const r=await fetch(ep,{method:'POST',headers:{Authorization:'vapid t='+await vapidJwt(env,new URL(ep).origin)+', k='+env.VAPID_PUBLIC,TTL:'7200',Urgency:'normal','Content-Length':'0'}});
    if(r.status==404||r.status==410)return {gone:true};
    return {ok:r.status>=200&&r.status<300,status:r.status}}catch(e){return {ok:false,status:0}}}

async function run(env){
  const now=Date.now();
  const {results}=await env.DB.prepare('SELECT * FROM push_subs WHERE nxt>0 AND nxt<=? ORDER BY nxt LIMIT ?').bind(now,PER_RUN).all();
  for(const r of results||[]){
    const d=decide(r,now);
    if(d.send){
      const x=await sendPush(env,r.ep);
      if(x.gone){await env.DB.prepare('DELETE FROM push_subs WHERE id=?').bind(r.id).run();continue}
      if(!x.ok){await env.DB.prepare('UPDATE push_subs SET nxt=? WHERE id=?').bind(now+15*6e4,r.id).run();continue} // átmeneti hiba: 15 perc múlva újra
      const u=d.upd;await env.DB.prepare('UPDATE push_subs SET n=?,lp=?,sent=?,ms=?,msg=?,nxt=? WHERE id=?').bind(u.n,u.lp,u.sent,u.ms,u.msg,u.nxt,r.id).run()
    }else if(d.upd.nxt!==r.nxt){await env.DB.prepare('UPDATE push_subs SET nxt=? WHERE id=?').bind(d.upd.nxt,r.id).run()}
  }
  // takarítás: 90 napja nem látott sor törlése (naponta egyszer, 03:00 UTC körül)
  const h=new Date(now);if(h.getUTCHours()==3&&h.getUTCMinutes()<10)await env.DB.prepare('DELETE FROM push_subs WHERE seen<?').bind(now-90*864e5).run();
}

export default{
  async scheduled(ev,env,ctx){ctx.waitUntil(run(env))},
  async fetch(){return new Response('spacemate-push ok')}
};
