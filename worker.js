// spacemate-push Worker: 10 percenként (cron) megnézi, kinek járt le a szükséglete, és üres Web Push-t küld.
// Kötések/titkok: DB (D1: theta-usage), VAPID_PUBLIC (változó), VAPID_D (TITOK), VAPID_SUB (változó, pl. mailto:te@példa.hu vagy https://spacemate.radvanszki.com)
const te=s=>new TextEncoder().encode(s);
const b64uBytes=b=>{const r=atob((b+'='.repeat((4-b.length%4)%4)).replace(/-/g,'+').replace(/_/g,'/'));return Uint8Array.from(r,c=>c.charCodeAt(0))};
const b64u=u=>btoa(String.fromCharCode(...new Uint8Array(u))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
// GAP: ugyanannak a lénynek két push között legalább ennyi; SPACE: bármelyik két push között legalább ennyi (hogy a másik lény pushának szövege ne írja felül az előzőét, mielőtt az megérkezne)
// LOOK: a most lejáró mellé a következő 30 percben lejáró szükségletek is bekerülnek ugyanabba a pushba (0 = kikapcsolva); PER_RUN: az ingyenes csomag ~50 kérés/futás korlátja miatt
const MISS_H=10,GAP=90*6e4,SPACE=10*6e4,OPENW=10*6e4,PER_RUN=20,LOOK=30*6e4;
const REPMAX=3,REP=2*36e5; // egy szükségletről legfeljebb REPMAX értesítés, az ismétlések között REP; a gondozás új esedékességet ad, azzal a számláló nullázódik
// a `sent` egy szükségletre: {d: az esedékesség, n: hány értesítés ment ki, t: az utolsó ideje}; a régi (szám) alakot kész (n=REPMAX) állapotnak vesszük
const sx=(v,d)=>v&&typeof v=='object'?(v.d===d?v:{d,n:0,t:0}):(v===d?{d,n:REPMAX,t:0}:{d,n:0,t:0});
const HOSTS=[/(^|\.)push\.apple\.com$/,/^fcm\.googleapis\.com$/,/(^|\.)push\.services\.mozilla\.com$/,/(^|\.)notify\.windows\.com$/];
const okEp=ep=>{try{const u=new URL(ep);return u.protocol=='https:'&&HOSTS.some(r=>r.test(u.hostname))}catch{return false}};

const QUIET=false; // true: 23–7 helyi idő között nem küldünk; false: éjjel is jöhet (a "Ne zavarj" kapcsolóval némítható)
const asleep=(t,tz)=>QUIET&&(()=>{const h=new Date(t-tz*6e4).getUTCHours();return h>=23||h<7})();
const wake=(t,tz)=>{if(!asleep(t,tz))return t;const l=new Date(t-tz*6e4);l.setUTCHours(7,0,0,0);let x=l.getTime()+tz*6e4;if(x<=t)x+=864e5;return x}; // pontosan 07:00 helyi idő

// a legkorábbi időpont, amikor ennek a sornak szólnia kell (lényenként külön számolva, a szünetekkel és az időközzel együtt)
function planNxt(r,dues,sent,lc){let m=Infinity;lc=lc||{};
  const base=Math.max(r.lp?r.lp+SPACE:0,r.seen+OPENW);
  for(const c in dues){let e=Infinity;for(const k in dues[c]){const d=dues[c][k],s=sx(sent[c+':'+k],d);
      const t=s.n==0?d:s.n<REPMAX?s.t+REP:Infinity;if(t<e)e=t}
    if(isFinite(e))m=Math.min(m,Math.max(e,base,lc[c]?lc[c]+GAP:0))}
  if(!r.ms)m=Math.min(m,Math.max(r.seen+MISS_H*36e5,base,r.lp?r.lp+GAP:0));
  if(!isFinite(m))return 0;
  return asleep(m,r.tz)?wake(m,r.tz):m}

// mit küldjünk most (vagy semmit): egy lénynek egy push, abban az összes esedékes szükséglete
function decide(r,now){
  if(r.dnd)return {send:null,upd:{nxt:0}}; // [DND] Ne zavarj: nem küldünk
  let dues={},sent={},lc={};try{dues=JSON.parse(r.dues||'{}');sent=JSON.parse(r.sent||'{}');lc=JSON.parse(r.lpc||'{}')}catch{}
  const can=!asleep(now,r.tz)&&(!r.lp||now-r.lp>=SPACE)&&now-r.seen>=OPENW;
  // lényenként: az első értesítésre váró (lejárt, vagy a LOOK időn belül lejáró) és az ismétlésre váró (REP telt el) szükségletek; legalább egynek tényleg esedékesnek kell lennie
  let best=null;
  for(const c in dues){if(lc[c]&&now-lc[c]<GAP)continue;
    const cs=[];let due=Infinity;
    for(const k in dues[c]){const d=dues[c][k],s=sx(sent[c+':'+k],d);
      if(s.n==0){if(d<=now+LOOK)cs.push({c,k,d,s});if(d<=now&&d<due)due=d}
      else if(s.n<REPMAX&&s.t+REP<=now){cs.push({c,k,d,s});if(s.t+REP<due)due=s.t+REP}}
    if(isFinite(due)&&(!best||due<best.due))best={due,cs}}
  if(can&&best){const s2={...sent},cs=best.cs;for(const x of cs)s2[x.c+':'+x.k]={d:x.d,n:x.s.n+1,t:now};
    const c=cs[0].c,lc2={...lc,[c]:now},r2={...r,n:r.n+1,lp:now};
    return {send:{c,k:cs[0].k},upd:{n:r2.n,lp:now,sent:JSON.stringify(s2),ms:r.ms,msg:JSON.stringify({i:cs.map(x=>[x.c,x.k])}),lpc:JSON.stringify(lc2),nxt:planNxt(r2,dues,s2,lc2)}}}
  if(can&&!r.ms&&now-r.seen>=MISS_H*36e5&&(!r.lp||now-r.lp>=GAP)){const c=r.last||'zizi',r2={...r,ms:1,lp:now};
    return {send:{c,k:'miss'},upd:{n:r.n,lp:now,sent:JSON.stringify(sent),ms:1,msg:JSON.stringify({c,k:'miss'}),lpc:JSON.stringify(lc),nxt:planNxt(r2,dues,sent,lc)}}}
  return {send:null,upd:{nxt:planNxt(r,dues,sent,lc)}}}

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
  const {results}=await env.DB.prepare('SELECT * FROM push_subs WHERE nxt>0 AND nxt<=? AND COALESCE(dnd,0)=0 ORDER BY nxt LIMIT ?').bind(now,PER_RUN).all();
  for(const r of results||[]){
    const d=decide(r,now);
    if(d.send){
      const x=await sendPush(env,r.ep);
      if(x.gone){await env.DB.prepare('DELETE FROM push_subs WHERE id=?').bind(r.id).run();continue}
      if(!x.ok){await env.DB.prepare('UPDATE push_subs SET nxt=? WHERE id=?').bind(now+15*6e4,r.id).run();continue} // átmeneti hiba: 15 perc múlva újra
      const u=d.upd;await env.DB.prepare('UPDATE push_subs SET n=?,lp=?,sent=?,ms=?,msg=?,nxt=?,lpc=? WHERE id=?').bind(u.n,u.lp,u.sent,u.ms,u.msg,u.nxt,u.lpc,r.id).run()
    }else if(d.upd.nxt!==r.nxt){await env.DB.prepare('UPDATE push_subs SET nxt=? WHERE id=?').bind(d.upd.nxt,r.id).run()}
  }
  // takarítás: 90 napja nem látott sor törlése (naponta egyszer, 03:00 UTC körül)
  const h=new Date(now);if(h.getUTCHours()==3&&h.getUTCMinutes()<10)await env.DB.prepare('DELETE FROM push_subs WHERE seen<?').bind(now-90*864e5).run();
}

export default{
  async scheduled(ev,env,ctx){ctx.waitUntil(run(env))},
  async fetch(req,env){
    const u=new URL(req.url);
    // [TESZT] kézi push egyedi szöveggel: /test?k=ADMIN_KEY&to=QP9o9&t=Cím&b=Szöveg  (to = a push-cím elejének egy darabja; a szabályokat nem veszi figyelembe)
    if(u.pathname=='/test'){
      const k=u.searchParams.get('k')||'',to=(u.searchParams.get('to')||'').replace(/[^A-Za-z0-9_-]/g,'');
      if(!env.ADMIN_KEY||k!==env.ADMIN_KEY||to.length<4)return new Response('nem',{status:403});
      const {results}=await env.DB.prepare('SELECT id,ep FROM push_subs WHERE ep LIKE ? LIMIT 5').bind('https://%/'+to+'%').all();
      const msg=JSON.stringify({t:(u.searchParams.get('t')||'Teszt').slice(0,60),b:(u.searchParams.get('b')||'Ez egy tesztértesítés.').slice(0,200)});
      const out=[];
      for(const r of results||[]){await env.DB.prepare('UPDATE push_subs SET msg=? WHERE id=?').bind(msg,r.id).run();const x=await sendPush(env,r.ep);out.push({cim:r.ep.slice(0,45),...x})}
      return new Response(JSON.stringify({talalat:out.length,out},null,1),{headers:{'content-type':'application/json'}})}
    return new Response('spacemate-push ok')}
};
