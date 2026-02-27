#!/usr/bin/env python3
"""
贵金属期货交易终端 · 构建脚本
================================
读取 data/YYYY-MM-DD/market_data.json → 生成 docs/index.html
双击 docs/index.html 即可打开，无需服务器。

用法:
  python build.py                # 构建今天的数据
  python build.py 2026-02-26     # 构建指定日期
"""
import os, sys, json
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# ═══════════════════════════════════════════════════════
#  HTML 模板 — raw string, 占位符: %%DATA%% / %%DATE%%
#  JS 全部用 + 拼接, 零反引号, 零 fetch, 零 f-string 冲突
# ═══════════════════════════════════════════════════════

TEMPLATE = r'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>贵金属期货交易终端</title>
<link rel="preconnect" href="https://fonts.font.im" crossorigin>
<link href="https://fonts.font.im/css2?family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet" media="print" onload="this.media='all'">
<noscript><link href="https://fonts.font.im/css2?family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet"></noscript>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#060a12;--bg2:#0b1120;--card:#111827;--card2:#0d1525;
  --bdr:rgba(255,255,255,0.06);--bdr2:rgba(255,255,255,0.1);
  --txt:#d1d5db;--txt2:#9ca3af;--dim:rgba(255,255,255,0.28);
  --gold:#f5a623;--green:#22c55e;--red:#ef4444;--blue:#3b82f6;
  --purple:#a78bfa;--cyan:#06b6d4;--pink:#ec4899;--orange:#f97316;
  --mono:'IBM Plex Mono','SF Mono','Consolas','Courier New',monospace;--sans:'PingFang SC','Microsoft YaHei','Noto Sans SC',system-ui,sans-serif;
}
body{background:var(--bg);color:var(--txt);font-family:var(--sans);overflow-x:hidden;min-height:100vh}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.08);border-radius:3px}
a{color:inherit;text-decoration:none}
button{font-family:inherit;cursor:pointer;border:none;outline:none}
canvas{display:block}

/* ─── Top Bar ─── */
.topbar{display:flex;align-items:center;justify-content:space-between;padding:10px 20px;
  border-bottom:1px solid var(--bdr);background:linear-gradient(90deg,rgba(245,166,35,0.05),transparent 50%)}
.logo{display:flex;align-items:center;gap:10px;font-weight:700;font-size:15px;color:#fff;user-select:none}
.logo-box{width:30px;height:30px;border-radius:6px;background:linear-gradient(135deg,var(--gold),#d4911e);
  display:flex;align-items:center;justify-content:center;color:var(--bg);font-size:14px;font-weight:800}
.mtabs{display:flex;gap:3px;margin-left:16px}
.mt{padding:5px 14px;border-radius:5px;background:transparent;color:var(--txt2);font-size:11.5px;font-weight:500;
  border:1px solid transparent;transition:all .15s}
.mt:hover{color:#fff;border-color:var(--bdr2)}
.mt.on{background:var(--gold);color:var(--bg);font-weight:700;border-color:var(--gold)}
.hdr-r{display:flex;align-items:center;gap:10px;font-size:11px;font-family:var(--mono);color:var(--txt2)}
.dot{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 6px rgba(34,197,94,0.5);
  animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}

/* ─── Price Bar ─── */
.pbar{display:flex;align-items:center;gap:16px;padding:8px 20px;border-bottom:1px solid var(--bdr);
  background:rgba(255,255,255,0.01);flex-wrap:wrap}
.pb-price{font-size:26px;font-weight:700;color:#fff;font-family:var(--mono);letter-spacing:-.5px}
.pb-unit{font-size:10px;color:var(--txt2);margin-left:2px}
.pb-chg{font-size:12px;font-weight:600;font-family:var(--mono)}
.pb-tag{display:inline-block;padding:2px 8px;border-radius:4px;font-size:9px;font-weight:700;margin-left:4px}
.pb-stat{font-size:10px;color:var(--txt2);font-family:var(--mono)}
.pb-stat b{color:#fff;font-weight:500}

/* ─── Chart ─── */
.chart-wrap{padding:8px 20px 12px;border-bottom:1px solid var(--bdr);background:var(--bg);position:relative}
.chart-leg{display:flex;gap:14px;font-size:10px;font-family:var(--mono);padding:4px 0 6px;color:var(--txt2)}
.chart-leg i{display:inline-block;width:14px;height:2px;vertical-align:middle;margin-right:3px;border-radius:1px}
#ktt{position:absolute;z-index:30;pointer-events:none;background:rgba(8,12,24,0.95);
  border:1px solid rgba(245,166,35,0.25);border-radius:6px;padding:8px 12px;font-size:11px;
  color:#ccc;font-family:var(--mono);min-width:152px;display:none;backdrop-filter:blur(8px)}

/* ─── Bottom Grid ─── */
.bot{display:grid;grid-template-columns:1fr 1fr;min-height:580px}
@media(max-width:1060px){.bot{grid-template-columns:1fr}}
.pl{border-right:1px solid var(--bdr);overflow-y:auto;max-height:780px}
.pr{overflow-y:auto;max-height:780px}

/* ─── Section ─── */
.sec{padding:14px 18px;border-bottom:1px solid var(--bdr)}
.stitle{font-size:12.5px;font-weight:700;color:#fff;margin-bottom:10px;display:flex;align-items:center;gap:7px}
.sbar{width:3px;height:13px;border-radius:2px}

/* ─── Contracts ─── */
.ctabs{display:flex;gap:4px;margin-bottom:10px}
.ctab{padding:4px 11px;border-radius:5px;border:1px solid var(--bdr);background:transparent;color:var(--txt2);
  font-size:10.5px;font-family:var(--mono);transition:all .15s}
.ctab.on{background:rgba(59,130,246,0.12);border-color:rgba(59,130,246,0.35);color:var(--blue);font-weight:600}

/* ─── Order Book ─── */
.obr{display:grid;grid-template-columns:34px 1fr 74px;gap:4px;padding:2px 0;font-family:var(--mono);font-size:10.5px;align-items:center}
.ob-fill{height:15px;position:relative;border-radius:2px;overflow:hidden}
.ob-bg{position:absolute;top:0;right:0;height:100%;border-radius:2px}
.ob-txt{position:relative;z-index:1;font-size:9.5px;color:rgba(255,255,255,0.65);padding:0 4px;line-height:15px;text-align:right}
.ob-sp{padding:4px 0;margin:2px 0;border-top:1px solid var(--bdr);border-bottom:1px solid var(--bdr);
  display:flex;justify-content:space-between;font-size:10.5px}
.c-info{display:grid;grid-template-columns:1fr 1fr;gap:3px 10px;margin-top:8px;font-size:10px;
  color:var(--txt2);font-family:var(--mono);padding:8px 10px;background:rgba(255,255,255,0.015);border-radius:6px}

/* ─── Indicators ─── */
.igrid{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}
.ibox{background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.035);border-radius:6px;padding:8px 10px}
.ibox-l{font-size:9px;color:var(--txt2);margin-bottom:4px;letter-spacing:.3px}
.ibox-v{font-size:14px;font-weight:700;font-family:var(--mono)}

/* ─── News ─── */
.ncats{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:12px}
.ncat{background:rgba(255,255,255,0.025);border:1px solid var(--bdr);border-radius:6px;padding:7px 10px;
  display:flex;align-items:center;gap:7px}
.ncat-ico{font-size:13px}.ncat-nm{font-size:10.5px;color:#fff;font-weight:600}
.ncat-ct{font-size:10px;color:var(--txt2)}
.nitem{padding:9px 0;border-bottom:1px solid rgba(255,255,255,0.035);transition:background .15s}
.nitem:hover{background:rgba(255,255,255,0.02)}.nitem:last-child{border-bottom:none}
.ndot{width:6px;height:6px;border-radius:50%;margin-top:5px;flex-shrink:0}
.ntitle{font-size:11px;color:var(--txt);line-height:1.55;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.nmeta{display:flex;justify-content:space-between;align-items:center;margin-top:4px;font-size:9px}
.nsrc{color:var(--cyan)}
.nsrc-link{color:var(--cyan);text-decoration:underline;text-underline-offset:2px;cursor:pointer}
.nimp{padding:1px 6px;border-radius:3px;font-size:8.5px;font-weight:700}

/* ─── Forecast ─── */
.fbox{border-radius:8px;padding:14px;margin-bottom:12px}
.fbox-hdr{display:flex;align-items:center;justify-content:space-between}
.fbox-lbl{font-size:9.5px;color:var(--txt2);text-transform:uppercase;letter-spacing:1px}
.fbox-dir{font-size:17px;font-weight:800;display:flex;align-items:center;gap:5px;margin-top:2px}
.fbox-conf{font-size:28px;font-weight:800;font-family:var(--mono)}
.pc-tab:hover{background:rgba(59,130,246,0.08)!important;color:#93c5fd!important}

/* ─── Analysis ─── */
.atxt{font-size:11px;color:var(--txt);line-height:1.85;padding:12px 14px;background:rgba(255,255,255,0.02);
  border:1px solid var(--bdr);border-radius:8px}
.atxt .hg{color:var(--green);font-weight:700}.atxt .hr{color:var(--red);font-weight:700}
.atxt .ho{color:var(--gold);font-weight:700}.atxt .hp{color:var(--purple);font-weight:700}
.atxt .hw{color:#fff;font-weight:600}

/* ─── Predictions ─── */
.prow{display:grid;grid-template-columns:85px 75px 1fr 52px 44px;gap:6px;padding:7px 10px;
  border-radius:6px;margin-bottom:3px;align-items:center;font-size:10.5px;
  background:rgba(255,255,255,0.012);border:1px solid rgba(255,255,255,0.025)}
.prow.hl{background:rgba(167,139,250,0.06);border-color:rgba(167,139,250,0.18)}
.pbar-outer{height:5px;border-radius:3px;background:rgba(255,255,255,0.04);overflow:hidden}
.pbar-fill{height:100%;border-radius:3px}

/* ─── AI Meters ─── */
.meters{display:flex;gap:6px;margin-top:10px}
.meter{flex:1;background:var(--card2);border:1px solid var(--bdr);border-radius:6px;padding:7px;text-align:center}
.meter-l{font-size:9px;color:var(--txt2);letter-spacing:.3px}
.meter-v{font-size:13px;font-weight:800;margin-top:2px}

/* ─── Topics ─── */
.tags{display:flex;flex-wrap:wrap;gap:4px;margin-top:10px}
.tag{padding:3px 9px;border-radius:4px;background:rgba(255,255,255,0.04);border:1px solid var(--bdr);
  font-size:9.5px;color:var(--txt2)}

/* ─── Algo Box ─── */
.algo{margin-top:10px;padding:10px 12px;background:rgba(167,139,250,0.035);border:1px solid rgba(167,139,250,0.1);
  border-radius:6px;font-size:10.5px;color:var(--txt2);line-height:1.7}
.algo b{color:var(--purple);font-weight:600}

/* ─── T+1/T+5 Grid ─── */
.tgrid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:10px}
.tcard{background:rgba(255,255,255,0.02);border:1px solid var(--bdr);border-radius:6px;padding:10px 12px;
  display:flex;flex-direction:column;min-height:90px}
.tcard-hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.tcard-metrics{font-size:9.5px;color:var(--txt2);font-family:var(--mono);line-height:1.8;flex:1}
</style>
</head>
<body>

<div class="topbar">
  <div style="display:flex;align-items:center">
    <div class="logo"><div class="logo-box">贵</div>贵金属终端</div>
    <div class="mtabs" id="mtabs"></div>
  </div>
  <div class="hdr-r"><span id="dtxt"></span><div class="dot"></div><span style="color:var(--green)">数据已加载</span></div>
</div>
<div class="pbar" id="pbar"></div>
<div class="chart-wrap">
  <div class="chart-leg">
    <span><i style="background:var(--gold)"></i>MA5</span>
    <span><i style="background:var(--blue)"></i>MA20</span>
    <span style="color:var(--green)">■ 涨</span>
    <span style="color:var(--red)">■ 跌</span>
    <span style="flex:1"></span>
    <span id="klbl" style="color:var(--dim)"></span>
  </div>
  <div style="position:relative"><canvas id="kc" height="370"></canvas><div id="ktt"></div></div>
</div>
<div class="bot">
  <div class="pl" id="panelL"></div>
  <div class="pr" id="panelR"></div>
</div>

<script>
var DATA = %%DATA%%;
var DSTR = "%%DATE%%";
var selM = Object.keys(DATA.metals)[0];
var selC = "";

document.getElementById("dtxt").textContent = DSTR + " " + (DATA.fetch_time||"").substring(11,19);

function gm(){ return DATA.metals[selM]; }
function fm(n){ return (n||0).toLocaleString(); }
function pickM(id){ selM=id; var cs=((gm()||{}).contracts||{}).contracts||[]; selC=cs[0]||""; renderAll(); }
function pickC(c){ selC=c; renderLeft(); }

/* ═══ K-LINE CHART ═══ */
function drawK(kline){
  var cv=document.getElementById("kc");if(!cv)return;
  var data=kline.slice(-80);if(!data.length)return;
  var W=cv.parentElement.clientWidth;
  if(!W||W<50){
    // 布局尚未完成, 延迟重试
    if(!drawK._retry)drawK._retry=0;
    if(drawK._retry++<10)setTimeout(function(){drawK(kline);},100);
    return;
  }
  drawK._retry=0;
  var H=370;
  cv.style.width=W+"px";cv.style.height=H+"px";
  var dp=window.devicePixelRatio||1;cv.width=W*dp;cv.height=H*dp;
  var c=cv.getContext("2d");c.scale(dp,dp);
  var T=12,R=52,B=32,L=6,cW=W-L-R,cH=H-T-B;
  var ap=[];for(var i=0;i<data.length;i++){ap.push(data[i].high,data[i].low);}
  var mn=Math.min.apply(null,ap),mx=Math.max.apply(null,ap),rg=mx-mn||1,bw=cW/data.length;
  c.clearRect(0,0,W,H);c.fillStyle="#060a12";c.fillRect(0,0,W,H);
  // grid
  c.strokeStyle="rgba(255,255,255,0.03)";c.lineWidth=1;
  for(var i=0;i<=6;i++){var y=T+(cH/6)*i;c.beginPath();c.moveTo(L,y);c.lineTo(W-R,y);c.stroke();
    c.fillStyle="rgba(255,255,255,0.28)";c.font="9.5px IBM Plex Mono,monospace";c.textAlign="left";
    c.fillText((mx-(rg/6)*i).toFixed(2),W-R+4,y+3);}
  // volume
  var mv=0;for(var i=0;i<data.length;i++)mv=Math.max(mv,data[i].volume||0);mv=mv||1;
  for(var i=0;i<data.length;i++){var d=data[i],x=L+i*bw+bw*.15,vh=((d.volume||0)/mv)*28;
    c.fillStyle=d.close>=d.open?"rgba(34,197,94,0.1)":"rgba(239,68,68,0.1)";c.fillRect(x,H-B-vh,bw*.7,vh);}
  // candles
  for(var i=0;i<data.length;i++){var d=data[i],x=L+i*bw,cx=x+bw/2,up=d.close>=d.open;
    var cl=up?"#22c55e":"#ef4444";
    var bt=T+((mx-Math.max(d.open,d.close))/rg)*cH,bb=T+((mx-Math.min(d.open,d.close))/rg)*cH;
    var wt=T+((mx-d.high)/rg)*cH,wb=T+((mx-d.low)/rg)*cH;
    c.strokeStyle=cl;c.lineWidth=1;c.beginPath();c.moveTo(cx,wt);c.lineTo(cx,wb);c.stroke();
    c.fillStyle=up?"rgba(34,197,94,0.75)":"rgba(239,68,68,0.75)";c.fillRect(x+bw*.2,bt,bw*.6,Math.max(bb-bt,1));}
  // MA
  var mc2=["#f5a623","#3b82f6"],mp=[5,20];
  for(var mi=0;mi<2;mi++){var p=mp[mi];c.strokeStyle=mc2[mi];c.lineWidth=1.2;c.beginPath();var s=false;
    for(var i=0;i<data.length;i++){if(i<p-1)continue;var sm=0;for(var j=i-p+1;j<=i;j++)sm+=data[j].close;var av=sm/p;
      var yy=T+((mx-av)/rg)*cH,xx=L+i*bw+bw/2;s?c.lineTo(xx,yy):(c.moveTo(xx,yy),s=true);}c.stroke();}
  // date labels
  c.fillStyle="rgba(255,255,255,0.22)";c.font="9px IBM Plex Mono,monospace";c.textAlign="center";
  var st=Math.max(1,Math.floor(data.length/10));
  for(var i=0;i<data.length;i+=st)c.fillText((data[i].date||"").substring(5,10),L+i*bw+bw/2,H-B+13);
  // label
  var last=data[data.length-1];
  document.getElementById("klbl").textContent=(last?last.date:"")+" 收:"+((last?last.close:0));
  // tooltip
  cv.onmousemove=function(e){var rc=cv.getBoundingClientRect(),mx2=e.clientX-rc.left;
    var idx=Math.floor((mx2-L)/bw),tt=document.getElementById("ktt");
    if(idx>=0&&idx<data.length){var d=data[idx];tt.style.display="block";
      tt.style.left=Math.min(mx2+10,W-165)+"px";tt.style.top=(e.clientY-rc.top-95)+"px";
      tt.innerHTML="<div style='color:#fff;font-weight:600;margin-bottom:3px'>"+d.date+"</div>"+
        "<div>开 <span style='color:var(--gold);float:right'>"+d.open+"</span></div>"+
        "<div>高 <span style='color:var(--green);float:right'>"+d.high+"</span></div>"+
        "<div>低 <span style='color:var(--red);float:right'>"+d.low+"</span></div>"+
        "<div>收 <span style='color:var(--blue);float:right'>"+d.close+"</span></div>"+
        "<div>量 <span style='float:right'>"+(d.volume||0).toLocaleString()+"</span></div>";}
    else tt.style.display="none";};
  cv.onmouseleave=function(){document.getElementById("ktt").style.display="none";};
}

/* ═══ Tab切换: 逐合约交割危机 ═══ */
function switchPcTab(el){
  var tabGroup=el.getAttribute("data-pctab");
  var idx=el.getAttribute("data-pcidx");
  // 切换tab样式
  document.querySelectorAll('.pc-tab[data-pctab="'+tabGroup+'"]').forEach(function(t){
    t.classList.remove("pc-tab-on");
    t.style.background="transparent";
    t.style.color="var(--txt2)";
    t.style.boxShadow="none";
  });
  el.classList.add("pc-tab-on");
  el.style.background="rgba(59,130,246,0.15)";
  el.style.color="#60a5fa";
  el.style.boxShadow="inset 0 -2px 0 #3b82f6";
  // 切换面板
  document.querySelectorAll('.pc-panel[data-pctab="'+tabGroup+'"]').forEach(function(p){
    p.style.display=p.getAttribute("data-pcidx")===idx?"":"none";
  });
}

/* ═══ RENDER ALL ═══ */
function renderAll(){
  try{renderTabs();}catch(e){console.error("renderTabs:",e);}
  try{renderPbar();}catch(e){console.error("renderPbar:",e);}
  try{renderLeft();}catch(e){console.error("renderLeft:",e);document.getElementById("panelL").innerHTML='<div style="color:red;padding:20px">左面板渲染错误: '+e.message+'</div>';}
  try{renderRight();}catch(e){console.error("renderRight:",e);document.getElementById("panelR").innerHTML='<div style="color:red;padding:20px">分析面板渲染错误: '+e.message+'</div>';}
  var kl=((gm()||{}).kline||{}).data||[];
  if(kl.length){
    requestAnimationFrame(function(){
      try{drawK(kl);}catch(e){console.error("drawK:",e);}
    });
  }
}

function renderTabs(){
  var h="",ids=Object.keys(DATA.metals);
  for(var k=0;k<ids.length;k++){var id=ids[k],m=DATA.metals[id],act=id===selM;
    h+='<button class="mt'+(act?" on":"")+'" onclick="pickM(\''+id+'\')">'+m.metal_name+" "+id+"</button>";}
  document.getElementById("mtabs").innerHTML=h;
}

function renderPbar(){
  var m=gm();if(!m)return;
  var kl=(m.kline||{}).data||[],last=kl[kl.length-1],prev=kl[kl.length-2];
  var price=(m.realtime||{}).last_price||(last?last.close:0)||0;
  var pc=prev?prev.close:price,chg=price-pc,pct=pc>0?(chg/pc*100).toFixed(2):"0.00",up=chg>=0;
  var ind=((m.indicators||{}).indicators)||{};
  var h='<span class="pb-price">'+price+'</span><span class="pb-unit">'+m.unit+'</span>';
  h+='<span class="pb-chg" style="color:'+(up?"var(--green)":"var(--red)")+'">'+(up?"▲ +":"▼ ")+chg.toFixed(2)+" ("+(up?"+":"")+pct+"%)</span>";
  h+='<span class="pb-tag" style="background:'+(up?"rgba(34,197,94,0.12);color:var(--green)":"rgba(239,68,68,0.12);color:var(--red)")+'">'+(up?"看涨":"看跌")+"</span>";
  h+='<span style="flex:1"></span>';
  h+='<span class="pb-stat">持仓 <b>'+fm(ind.open_interest)+'手</b></span>';
  h+='<span class="pb-stat">成交 <b>'+fm(ind.daily_volume)+'手</b></span>';
  var invDisp=ind.cme_total_oz>0?fm(ind.cme_total_oz)+" oz":fm(ind.total_inventory);
  h+='<span class="pb-stat">库存 <b>'+invDisp+'</b></span>';
  var pcFirst=(m.per_contract_crisis||[])[0]||{};
  var dcP=pcFirst.probability_pct||(m.delivery_crisis||{}).probability_pct||0;
  var dcC=pcFirst.level_color||(m.delivery_crisis||{}).level_color||"#666";
  var dcN=pcFirst.contract||"";
  h+='<span class="pb-stat">危机率'+(dcN?' <span style="font-size:9px;opacity:0.7">'+dcN+'</span>':'')+' <b style="color:'+dcC+'">'+dcP.toFixed(0)+'%</b></span>';
  document.getElementById("pbar").innerHTML=h;
}

/* ═══ LEFT PANEL: Contracts + Indicators + News ═══ */
function renderLeft(){
  var m=gm();if(!m)return;
  var cd=m.contracts||{},codes=cd.contracts||[];
  if(!selC||codes.indexOf(selC)<0)selC=codes[0]||"";
  var ct=(cd.data||{})[selC]||{},cup=(ct.change||0)>=0;
  var ind=((m.indicators||{}).indicators)||{};
  var news=((m.news||{}).news)||[];
  var h="";

  /* ── Contracts + Orderbook ── */
  h+='<div class="sec"><div class="stitle"><div class="sbar" style="background:var(--blue)"></div>期货合约</div>';
  h+='<div class="ctabs">';
  for(var i=0;i<codes.length;i++)h+='<button class="ctab'+(codes[i]===selC?" on":"")+'" onclick="pickC(\''+codes[i]+'\')">'+codes[i]+"</button>";
  h+="</div>";
  if(ct.last_price){
    var asks=ct.asks||[],bids=ct.bids||[],mxv=1;
    for(var i=0;i<asks.length;i++)mxv=Math.max(mxv,asks[i].volume);
    for(var i=0;i<bids.length;i++)mxv=Math.max(mxv,bids[i].volume);
    h+='<div style="font-size:10px;color:var(--txt2);margin-bottom:5px">'+selC+" · 五档盘口</div>";
    for(var i=asks.length-1;i>=0;i--){var a=asks[i],p=(a.volume/mxv*100).toFixed(0);
      h+='<div class="obr"><span style="font-size:9.5px;color:var(--txt2)">卖'+(i+1)+"</span>";
      h+='<span style="color:var(--red);font-weight:500">'+a.price+"</span>";
      h+='<div class="ob-fill"><div class="ob-bg" style="width:'+p+"%;background:rgba(239,68,68,0.1)\"></div>";
      h+='<div class="ob-txt">'+a.volume+"</div></div></div>";}
    var sp=asks.length&&bids.length?(asks[0].price-bids[0].price).toFixed(2):"—";
    h+='<div class="ob-sp"><span style="color:var(--txt2)">价差</span><span style="color:var(--gold);font-weight:600;font-family:var(--mono)">'+sp+"</span></div>";
    for(var i=0;i<bids.length;i++){var b=bids[i],p=(b.volume/mxv*100).toFixed(0);
      h+='<div class="obr"><span style="font-size:9.5px;color:var(--txt2)">买'+(i+1)+"</span>";
      h+='<span style="color:var(--green);font-weight:500">'+b.price+"</span>";
      h+='<div class="ob-fill"><div class="ob-bg" style="width:'+p+"%;background:rgba(34,197,94,0.1)\"></div>";
      h+='<div class="ob-txt">'+b.volume+"</div></div></div>";}
    h+='<div class="c-info">';
    h+='<div>最新价 <span style="float:right;color:#fff">'+ct.last_price+"</span></div>";
    h+='<div>涨跌幅 <span style="float:right;color:'+(cup?"var(--green)":"var(--red)")+'">'+(cup?"+":"")+(ct.change_pct||0)+"%</span></div>";
    h+='<div>今开 <span style="float:right;color:#fff">'+(ct.open||"—")+"</span></div>";
    h+='<div>昨收 <span style="float:right;color:#fff">'+(ct.prev_close||"—")+"</span></div>";
    h+='<div>最高 <span style="float:right;color:var(--green)">'+(ct.high||"—")+"</span></div>";
    h+='<div>最低 <span style="float:right;color:var(--red)">'+(ct.low||"—")+"</span></div>";
    h+='<div>成交量 <span style="float:right;color:#fff">'+fm(ct.volume)+"手</span></div>";
    h+='<div>持仓量 <span style="float:right;color:#fff">'+fm(ct.open_interest)+"手</span></div></div>";
  } else h+='<div style="text-align:center;padding:24px;color:var(--txt2);font-size:12px">暂无盘口数据</div>';
  h+="</div>";

  /* ── Indicators ── */
  h+='<div class="sec"><div class="stitle"><div class="sbar" style="background:var(--gold)"></div>关键指标';
  // 数据来源标签
  var hasCME=ind.cme_registered_oz>0;
  if(hasCME){
    var cmeUnit=ind.cme_unit||"troy oz";
    var cmeDate=ind.cme_date||"";
    h+='<span style="font-size:8px;color:var(--cyan);margin-left:8px;font-weight:400">CME/COMEX · '+cmeUnit;
    if(cmeDate) h+=' · '+cmeDate;
    h+='</span>';
  } else {
    h+='<span style="font-size:8px;color:var(--txt2);margin-left:8px;font-weight:400">SHFE/SGE</span>';
  }
  h+='</div>';
  h+='<div class="igrid">';
  // CME: 全部按原始盎司/短吨显示; 非CME: 按国内单位
  if(hasCME){
    var u=ind.cme_unit||"oz";
    var items=[
      {l:"注册仓单",v:fm(ind.cme_registered_oz)+" "+u,c:"var(--gold)"},
      {l:"合格库存",v:fm(ind.cme_eligible_oz)+" "+u,c:"var(--green)"},
      {l:"总库存",v:fm(ind.cme_total_oz)+" "+u,c:"var(--blue)"},
      {l:"库存变化",v:((ind.inventory_change||0)>0?"+":"")+fm(ind.inventory_change),c:(ind.inventory_change||0)>=0?"var(--green)":"var(--red)"},
      {l:"持仓量(SHFE)",v:fm(ind.open_interest)+"手",c:"var(--purple)"},
      {l:"多空比",v:String(ind.position_ratio||"—"),c:(ind.position_ratio||0)>1?"var(--green)":"var(--red)"}
    ];
  } else {
    var items=[
      {l:"总库存",v:fm(ind.total_inventory),c:"var(--blue)"},
      {l:"注册库存",v:fm(ind.registered_inventory),c:"var(--gold)"},
      {l:"合格库存",v:fm(ind.qualified_inventory),c:"var(--green)"},
      {l:"库存变化",v:((ind.inventory_change||0)>0?"+":"")+fm(ind.inventory_change),c:(ind.inventory_change||0)>=0?"var(--green)":"var(--red)"},
      {l:"持仓量",v:fm(ind.open_interest)+"手",c:"var(--purple)"},
      {l:"多空比",v:String(ind.position_ratio||"—"),c:(ind.position_ratio||0)>1?"var(--green)":"var(--red)"}
    ];
  }
  for(var i=0;i<items.length;i++){var it=items[i];
    h+='<div class="ibox"><div class="ibox-l">'+it.l+'</div><div class="ibox-v" style="color:'+it.c+'">'+(it.v||"—")+"</div></div>";}
  h+="</div></div>";

  /* ── News + Dual Sentiment ── */
  var sent=(m.news||{}).sentiment||{};
  var sentLabel=sent.label||"中性",sentConf=sent.confidence||50,sentFused=sent.fused_score||0;
  var sentFB=sent.finbert_score||0,sentLM=sent.lm_score||0;
  var sentAgree=sent.agreement||"";
  h+='<div class="sec"><div class="stitle" style="justify-content:space-between"><div style="display:flex;align-items:center;gap:7px">';
  h+='<div class="sbar" style="background:var(--pink)"></div>'+m.metal_name+"相关资讯</div>";
  // 双算法情绪摘要
  var slClr=sentLabel.indexOf("利好")>=0?"var(--green)":sentLabel.indexOf("利空")>=0?"var(--red)":"var(--gold)";
  h+='<span style="font-size:11px;font-weight:700;color:'+slClr+'">'+sentConf+"% "+sentLabel+"</span></div>";

  // 双算法详情卡片
  h+='<div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);border-radius:6px;padding:8px 10px;margin-bottom:8px">';
  h+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">';
  h+='<span style="font-size:10px;font-weight:700;color:#fff">FinBERT + LM 双算法</span>';
  var agClr=sentAgree==="同向"?"var(--green)":sentAgree==="分歧"?"var(--red)":"var(--gold)";
  h+='<span style="font-size:9px;padding:1px 6px;border-radius:3px;background:rgba(255,255,255,0.06);color:'+agClr+'">'+sentAgree+"</span></div>";
  // 两算法分数条
  var fbPct=Math.round((sentFB+1)*50),lmPct=Math.round((sentLM+1)*50);
  h+='<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">';
  h+='<div><div style="font-size:9px;color:var(--txt2);margin-bottom:2px">FinBERT (α=0.6)</div>';
  h+='<div style="display:flex;align-items:center;gap:4px"><div style="flex:1;height:4px;background:rgba(255,255,255,0.08);border-radius:2px;overflow:hidden">';
  h+='<div style="width:'+fbPct+'%;height:100%;background:'+(sentFB>0?"var(--green)":sentFB<0?"var(--red)":"var(--gold)")+';border-radius:2px"></div></div>';
  h+='<span style="font-size:9px;font-family:var(--mono);color:var(--txt2)">'+(sentFB>=0?"+":"")+sentFB.toFixed(3)+"</span></div></div>";
  h+='<div><div style="font-size:9px;color:var(--txt2);margin-bottom:2px">LM词典 (α=0.4)</div>';
  h+='<div style="display:flex;align-items:center;gap:4px"><div style="flex:1;height:4px;background:rgba(255,255,255,0.08);border-radius:2px;overflow:hidden">';
  h+='<div style="width:'+lmPct+'%;height:100%;background:'+(sentLM>0?"var(--green)":sentLM<0?"var(--red)":"var(--gold)")+';border-radius:2px"></div></div>';
  h+='<span style="font-size:9px;font-family:var(--mono);color:var(--txt2)">'+(sentLM>=0?"+":"")+sentLM.toFixed(3)+"</span></div></div></div></div>";

  var pn=0,nn=0,un=0;
  for(var i=0;i<news.length;i++){if(news[i].impact==="positive")pn++;else if(news[i].impact==="negative")nn++;else un++;}
  h+='<div class="ncats">';
  h+='<div class="ncat"><span class="ncat-ico">📈</span><div><div class="ncat-nm">利多新闻</div><div class="ncat-ct">'+pn+" 篇</div></div></div>";
  h+='<div class="ncat"><span class="ncat-ico">📉</span><div><div class="ncat-nm">利空新闻</div><div class="ncat-ct">'+nn+" 篇</div></div></div>";
  h+='<div class="ncat"><span class="ncat-ico">📊</span><div><div class="ncat-nm">中性资讯</div><div class="ncat-ct">'+un+" 篇</div></div></div>";
  h+='<div class="ncat"><span class="ncat-ico">📰</span><div><div class="ncat-nm">资讯合计</div><div class="ncat-ct">'+news.length+" 篇</div></div></div></div>";

  for(var i=0;i<news.length;i++){var n=news[i];
    var dc=n.impact==="positive"?"var(--green)":n.impact==="negative"?"var(--red)":"var(--gold)";
    var il=n.impact==="positive"?"利多":n.impact==="negative"?"利空":"中性";
    var ib=n.impact==="positive"?"rgba(34,197,94,0.12);color:var(--green)":n.impact==="negative"?"rgba(239,68,68,0.12);color:var(--red)":"rgba(245,166,35,0.12);color:var(--gold)";
    var ss=n.sentiment_score!==undefined?((n.sentiment_score>=0?"+":"")+n.sentiment_score.toFixed(2)):"";
    var url=n.url||"";
    h+='<div class="nitem"><div style="display:flex;align-items:flex-start;gap:7px">';
    h+='<div class="ndot" style="background:'+dc+'"></div><div style="flex:1">';
    if(url) h+='<a href="'+url+'" target="_blank" class="ntitle" style="text-decoration:underline;text-underline-offset:2px">'+n.title+"</a>";
    else h+='<div class="ntitle">'+n.title+"</div>";
    h+='<div class="nmeta">';
    if(url) h+='<a href="'+url+'" target="_blank" class="nsrc-link">'+(n.source||"查看原文")+" ↗</a>";
    else h+='<span class="nsrc">'+(n.source||"")+"</span>";
    h+='<span style="display:flex;align-items:center;gap:6px">';
    if(n.date) h+='<span style="color:var(--dim)">'+n.date+"</span>";
    h+='<span class="nimp" style="background:'+ib+'">'+il+(ss?" "+ss:"")+"</span></span></div></div></div></div>";}
  h+="</div>";

  document.getElementById("panelL").innerHTML=h;
}

/* ═══ RIGHT PANEL: IDAF Forecast + Analysis + Predictions ═══ */
function renderRight(){
  var m=gm();if(!m)return;
  var pred=m.predictions||{},preds=pred.predictions||[],ana=pred.analysis||{};
  var ind=((m.indicators||{}).indicators)||{};
  var kl=(m.kline||{}).data||[],last=kl[kl.length-1],prev=kl[kl.length-2];
  var price=(m.realtime||{}).last_price||(last?last.close:0)||0;
  var prevC=prev?prev.close:price,isUp=price>=prevC;
  var news=((m.news||{}).news)||[];
  var maxP=0;for(var i=0;i<preds.length;i++)maxP=Math.max(maxP,preds[i].probability||0);
  var best={};for(var i=0;i<preds.length;i++)if(preds[i].probability===maxP)best=preds[i];
  var pn=0,nn2=0;for(var i=0;i<news.length;i++){if(news[i].impact==="positive")pn++;else if(news[i].impact==="negative")nn2++;}
  var sent2=(m.news||{}).sentiment||{};
  var sentLabel2=sent2.label||"中性",sentConf2=sent2.confidence||50,sentFused2=sent2.fused_score||0;
  var sentL=sentLabel2.indexOf("利好")>=0?"偏多":sentLabel2.indexOf("利空")>=0?"偏空":"中性";
  var sentS=sentFused2.toFixed(3);
  var dir=ana.overall_direction||sentL;
  var dirUp=dir.indexOf("多")>=0;
  var conf=ana.confidence||50;
  var ma5=0,ma20=0;
  if(kl.length>=5){for(var i=kl.length-5;i<kl.length;i++)ma5+=kl[i].close;ma5/=5;}
  if(kl.length>=20){for(var i=kl.length-20;i<kl.length;i++)ma20+=kl[i].close;ma20/=20;}
  var maTrend=ma5>ma20?"多头排列":"空头排列";
  var cd=m.contracts||{},codes=cd.contracts||[],cdata=cd.data||{};
  var nearP=codes.length>0?((cdata[codes[0]]||{}).last_price||0):0;
  var farP=codes.length>1?((cdata[codes[codes.length-1]]||{}).last_price||0):0;
  var contango=farP>nearP;

  var h="";

  /* ── Forecast Header ── */
  h+='<div class="sec"><div class="stitle" style="justify-content:space-between"><div style="display:flex;align-items:center;gap:7px">';
  h+='<div class="sbar" style="background:var(--cyan)"></div>AI 综合研判</div>';
  h+='<span style="font-size:10px;color:var(--txt2);font-weight:400">IDAF v1.0 · 三因子模型</span></div>';

  var fclr=dirUp?"var(--green)":"var(--red)";
  var fbg=dirUp?"rgba(34,197,94,0.04)":"rgba(239,68,68,0.04)";
  var fbdr=dirUp?"rgba(34,197,94,0.15)":"rgba(239,68,68,0.15)";
  if(dir.indexOf("中性")>=0||dir.indexOf("震荡")>=0){fclr="var(--gold)";fbg="rgba(245,166,35,0.04)";fbdr="rgba(245,166,35,0.15)";}
  h+='<div class="fbox" style="background:'+fbg+";border:1px solid "+fbdr+'">';
  h+='<div class="fbox-hdr"><div><div class="fbox-lbl">综合方向</div>';
  h+='<div class="fbox-dir" style="color:'+fclr+'">'+(dirUp?"↑":dir.indexOf("空")>=0?"↓":"↔")+" "+dir+"</div></div>";
  h+='<div class="fbox-conf" style="color:'+fclr+'">'+conf+"%</div></div></div>";

  /* ── 逐合约交割危机概率 (Tab切换) ── */
  var pcArr=m.per_contract_crisis||[];
  var dc=m.delivery_crisis||{};
  var pcTabId="pctab_"+selC;

  if(pcArr.length>0){
    // Tab 按钮行
    h+='<div style="display:flex;gap:0;margin-top:8px;border:1px solid rgba(255,255,255,0.12);border-radius:6px;overflow:hidden;background:rgba(0,0,0,0.2)">';
    for(var pi=0;pi<pcArr.length;pi++){
      var pc0=pcArr[pi];
      var tp=pc0.probability_pct||0;
      var tc2=tp>=50?"var(--red)":tp>=30?"var(--gold)":"var(--green)";
      h+='<div class="pc-tab'+(pi===0?' pc-tab-on':'')+'" data-pctab="'+pcTabId+'" data-pcidx="'+pi+'" onclick="switchPcTab(this)" style="flex:1;text-align:center;padding:7px 4px;cursor:pointer;font-size:11px;font-weight:600;font-family:var(--mono);transition:all .15s;'+(pi===0?'background:rgba(59,130,246,0.15);color:#60a5fa;box-shadow:inset 0 -2px 0 #3b82f6':'color:var(--txt2)')+'">';
      h+=pc0.contract||"";
      h+=' <span style="font-size:9px;font-weight:400;color:'+tc2+'">'+tp.toFixed(0)+'%</span>';
      h+='</div>';
    }
    h+='</div>';

    // 每个合约的详情面板
    for(var pi=0;pi<pcArr.length;pi++){
      var pc=pcArr[pi];
      var pcProb=pc.probability_pct||0;
      var pcLevel=pc.level||"未知";
      var pcColor=pc.level_color||"#666";
      var pcFactors=pc.factors||{};
      var pcSummary=pc.summary||"";
      var pcDM=pc.delivery_month||"";

      var pcArrow=pcProb>=50?"↑":pcProb>=30?"↗":pcProb>=15?"→":"↓";
      var pcBg2=pcProb>=50?"rgba(239,68,68,0.04)":pcProb>=30?"rgba(245,166,35,0.04)":"rgba(34,197,94,0.04)";
      var pcBdr2=pcProb>=50?"rgba(239,68,68,0.15)":pcProb>=30?"rgba(245,166,35,0.15)":"rgba(34,197,94,0.15)";

      h+='<div class="pc-panel" data-pctab="'+pcTabId+'" data-pcidx="'+pi+'" style="'+(pi===0?'':'display:none;')+'">';
      h+='<div class="fbox" style="background:'+pcBg2+";border:1px solid "+pcBdr2+';margin-top:4px">';
      h+='<div class="fbox-hdr"><div><div class="fbox-lbl">交割危机 <span style="font-weight:400;color:var(--txt2)">'+pcDM+'</span></div>';
      h+='<div class="fbox-dir" style="color:'+pcColor+'">'+pcArrow+' '+pcLevel+'</div></div>';
      h+='<div class="fbox-conf" style="color:'+pcColor+'">'+pcProb.toFixed(0)+"%</div></div>";

      // 概率条
      h+='<div style="padding:6px 10px 0">';
      var barBg="linear-gradient(90deg, #22c55e 0%, #22c55e 15%, #eab308 30%, #f97316 55%, #ef4444 80%, #ef4444 100%)";
      h+='<div style="position:relative;height:4px;border-radius:2px;background:rgba(255,255,255,0.06);margin-bottom:4px;overflow:visible">';
      h+='<div style="position:absolute;top:0;left:0;width:100%;height:100%;border-radius:2px;background:'+barBg+';opacity:0.2"></div>';
      h+='<div style="position:absolute;top:0;left:0;width:'+Math.min(pcProb,100)+'%;height:100%;border-radius:2px;background:'+barBg+'"></div>';
      h+='<div style="position:absolute;top:-3px;left:calc('+Math.min(pcProb,100)+'% - 4px);width:8px;height:10px;border-radius:2px;background:'+pcColor+';box-shadow:0 0 6px '+pcColor+'66"></div>';
      h+='</div>';

      // 五因子 grid
      h+='<div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr 1fr;gap:2px;margin:4px 0">';
      var fnames=[
        {k:"coverage",n:"覆盖率",ic:"📦",w:30},
        {k:"term_structure",n:"期限",ic:"📈",w:25},
        {k:"inventory_flow",n:"库存",ic:"🔄",w:20},
        {k:"timing",n:"时点",ic:"⏱",w:10},
        {k:"stress",n:"压力",ic:"⚡",w:15}
      ];
      for(var fi=0;fi<fnames.length;fi++){
        var fn=fnames[fi],fd=pcFactors[fn.k]||{};
        var fs=fd.score||0,fPct=(fs*100).toFixed(0);
        var fc2=fs>0.6?"var(--red)":fs>0.35?"var(--gold)":"var(--green)";
        h+='<div style="text-align:center;padding:3px 1px;background:rgba(255,255,255,0.02);border:1px solid rgba(255,255,255,0.04);border-radius:3px">';
        h+='<div style="font-size:7px;color:var(--txt2)">'+fn.ic+' '+fn.n+'</div>';
        h+='<div style="font-size:13px;font-weight:700;color:'+fc2+';font-family:var(--mono)">'+fPct+'</div>';
        h+='<div style="font-size:6.5px;color:var(--dim)">'+fn.w+'%</div></div>';
      }
      h+='</div>';

      // 因子明细 — 2列
      h+='<div style="display:grid;grid-template-columns:1fr 1fr;gap:1px 8px;font-size:8.5px;color:var(--txt2);line-height:1.5">';
      for(var fi=0;fi<fnames.length;fi++){
        var fn=fnames[fi],fd=pcFactors[fn.k]||{};
        if(fd.label) h+='<div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis"><span style="color:var(--dim)">'+fn.ic+' '+fn.n+':</span> '+fd.label+(fd.source?' <span style="color:var(--dim)">['+fd.source+']</span>':'')+'</div>';
      }
      h+='</div>';

      if(pcSummary){
        h+='<div style="margin-top:4px;padding:3px 6px;background:'+pcColor+'0a;border-left:2px solid '+pcColor+';font-size:8.5px;color:var(--txt);border-radius:0 3px 3px 0">';
        h+=pcSummary+'</div>';
      }

      h+='</div></div>';
      h+='</div>';
    }
  } else {
    var dcProb=dc.probability_pct||0;
    var dcLevel=dc.level||"未知";
    var dcColor=dc.level_color||"#666";
    var dcArrow=dcProb>=50?"↑":dcProb>=30?"↗":dcProb>=15?"→":"↓";
    var dcBg2=dcProb>=50?"rgba(239,68,68,0.04)":dcProb>=30?"rgba(245,166,35,0.04)":"rgba(34,197,94,0.04)";
    var dcBdr2=dcProb>=50?"rgba(239,68,68,0.15)":dcProb>=30?"rgba(245,166,35,0.15)":"rgba(34,197,94,0.15)";
    h+='<div class="fbox" style="background:'+dcBg2+";border:1px solid "+dcBdr2+';margin-top:6px">';
    h+='<div class="fbox-hdr"><div><div class="fbox-lbl">交割危机概率</div>';
    h+='<div class="fbox-dir" style="color:'+dcColor+'">'+dcArrow+' '+dcLevel+'</div></div>';
    h+='<div class="fbox-conf" style="color:'+dcColor+'">'+dcProb.toFixed(0)+"%</div></div></div>";
  }

  h+='<div style="margin:4px 0 6px;font-size:7.5px;color:var(--dim);text-align:center">CME Deliverable Supply (Reg+50%Elig) · CFTC监控口径 · 逐合约FND推算</div>';

  /* ── 三因子诊断面板 ── */
  var dscr=ana.dscr||0,sqR=ana.squeeze_risk||0,incS=ana.incentive_score||0,cvR=ana.convergence_risk||0;
  var isEstimated=ana.deliverable_estimated||false;
  var hasCME=ind.cme_registered_oz>0;
  var isCME=ana.deliverable_cme||hasCME;
  h+='<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;margin:8px 0">';
  // DSCR
  var dscrClr=dscr<1?"var(--red)":dscr<2?"var(--gold)":"var(--green)";
  h+='<div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);border-radius:6px;padding:8px;text-align:center">';
  h+='<div style="font-size:9px;color:var(--txt2);margin-bottom:3px">DSCR 覆盖率</div>';
  h+='<div style="font-size:18px;font-weight:800;color:'+dscrClr+';font-family:var(--mono)">'+dscr.toFixed(1)+'x</div>';
  h+='<div style="font-size:8.5px;color:var(--txt2)">'+(ana.dscr_status||"—")+(isCME?" · CME":isEstimated?" · 估算":"")+'</div></div>';
  // Basis-Carry
  var incClr=incS>0.1?"var(--green)":incS<-0.1?"var(--red)":"var(--gold)";
  h+='<div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);border-radius:6px;padding:8px;text-align:center">';
  h+='<div style="font-size:9px;color:var(--txt2);margin-bottom:3px">交割激励</div>';
  h+='<div style="font-size:18px;font-weight:800;color:'+incClr+';font-family:var(--mono)">'+(incS>=0?"+":"")+incS.toFixed(2)+'</div>';
  h+='<div style="font-size:8.5px;color:var(--txt2)">'+(ana.incentive_status||"—")+'</div></div>';
  // Squeeze/Convergence
  var sqClr=sqR>0.5?"var(--red)":sqR>0.3?"var(--gold)":"var(--green)";
  h+='<div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);border-radius:6px;padding:8px;text-align:center">';
  h+='<div style="font-size:9px;color:var(--txt2);margin-bottom:3px">挤仓风险</div>';
  h+='<div style="font-size:18px;font-weight:800;color:'+sqClr+';font-family:var(--mono)">'+(sqR*100).toFixed(0)+'%</div>';
  h+='<div style="font-size:8.5px;color:var(--txt2)">'+(ana.squeeze_risk_level||"—")+'</div></div>';
  h+='</div>';

  /* ── 综合分析文本 ── */
  h+='<div class="atxt">';
  // DSCR分析
  if(ana.deliverable_supply>0){
    // 如果有CME原始数据, 优先显示CME单位
    var supplyDisp=hasCME?fm(ind.cme_registered_oz)+" "+(ind.cme_unit||"oz"):fm(ana.deliverable_supply);
    var srcTag=hasCME?"<span style='color:var(--cyan);font-size:9px'>(CME/COMEX)</span>":
               isCME?"<span style='color:var(--cyan);font-size:9px'>(CME)</span>":
               isEstimated?"<span style='color:var(--gold);font-size:9px'>(估算)</span>":"";
    h+="<strong>① 供给覆盖率(DSCR):</strong> 可交割供给(注册仓单)<span class='hw'>"+supplyDisp+"</span>"+srcTag;
    h+="，潜在交割需求<span class='hw'>"+fm(ana.delivery_demand)+"</span>";
    h+="，覆盖率<span class='"+(dscr<1.5?"hr":"hg")+"'>"+dscr.toFixed(2)+"x</span>";
    h+="("+(ana.dscr_status||"")+")";
    if(sqR>0.4){h+="，<span class='hr'>挤仓压力较大</span>";}
    else if(sqR<0.2){h+="，交割风险可控";}
    h+="。";}
  // Basis-Carry分析
  if(ana.basis_abs!==undefined){
    h+=" <strong>② 基差-持有成本:</strong> 基差<span class='hw'>"+(ana.basis_abs>=0?"+":"")+ana.basis_abs+"</span>";
    h+="("+ana.basis_status+")";
    h+="，理论无套利基差<span class='hw'>"+ana.fair_basis+"</span>";
    if(Math.abs(ana.basis_deviation||0)>0.5){
      h+="，<span class='"+(ana.basis_deviation>0?"hr":"hg")+"'>偏离"+(ana.basis_deviation>0?"偏高":"偏低")+Math.abs(ana.basis_deviation).toFixed(2)+"</span>";
    }
    h+="，交割激励"+(incS>0?"正值(利于交割)":"负值(不利交割)");
    if(cvR>0.5){h+="，<span class='hr'>收敛风险偏高</span>";}
    h+="。";}
  // 技术面
  h+=" <strong>③ 压力测试:</strong> ";
  if(ana.momentum_5d!==undefined){
    h+="5日动量<span class='"+(ana.momentum_5d>0?"hg":"hr")+"'>"+(ana.momentum_5d>0?"+":"")+ana.momentum_5d+"%</span>";}
  if(ana.volatility_annual){h+="，年化波动率<span class='hw'>"+ana.volatility_annual+"%</span>";}
  if(ma5>0&&ma20>0){h+="，均线<span class='hw'>"+maTrend+"</span>";}
  h+="。";
  // 新闻(双算法)
  if(news.length>0){
    h+=" <strong>④ 舆情(FinBERT+LM):</strong> "+news.length+"条新闻";
    h+="，<span class='hg'>"+pn+"条利多</span>/<span class='hr'>"+nn2+"条利空</span>";
    h+="，情绪分析<span class='"+(sentLabel2.indexOf("利好")>=0?"hg":sentLabel2.indexOf("利空")>=0?"hr":"ho")+"'>"+sentConf2+"% "+sentLabel2+"</span>";
    h+=" (FinBERT="+sent2.finbert_score+", LM="+sent2.lm_score+", "+(sent2.agreement||"")+")。";}
  // 结论
  h+=" <strong>结论:</strong> 概率加权预估交割率<span class='hp'>"+(ana.expected_delivery_rate||"—")+"%</span>";
  h+="，预估价格<span class='hp'>"+(ana.expected_price||"—")+"</span>";
  h+="，上行概率<span class='hg'>"+(ana.upside_probability||0)+"%</span> vs 下行概率<span class='hr'>"+(ana.downside_probability||0)+"%</span>";
  h+="，综合研判<span class='hp'>"+dir+"</span>。";
  h+="</div>";

  /* ── T+1 / T+5 ── */
  h+='<div class="tgrid">';
  var t1c=isUp?"var(--green)":"var(--red)";
  var t1dir=isUp?"偏多":"偏空";
  h+='<div class="tcard"><div class="tcard-hdr"><span style="font-size:11px;font-weight:700;color:#fff">T+1</span>';
  h+='<span style="font-size:11px;font-weight:700;color:'+t1c+'">'+(isUp?"↑":"↓")+" "+t1dir+"</span>";
  h+='<span style="font-size:13px;font-weight:800;color:'+t1c+';font-family:var(--mono)">'+conf+"%</span></div>";
  h+='<div class="tcard-metrics">';
  h+="DSCR覆盖: "+((dscr||0).toFixed?dscr.toFixed(1)+"x":"—");
  h+="<br>挤仓风险: "+(ana.squeeze_risk_level||"—");
  h+="<br>情绪: "+sentConf2+"% "+sentLabel2+"</div></div>";
  var t5conf=Math.min(95,parseInt(conf)+Math.round(Math.abs(pn-nn2)*2));
  var t5dir=dir;var t5c=t5dir.indexOf("多")>=0?"var(--green)":t5dir.indexOf("空")>=0?"var(--red)":"var(--gold)";
  h+='<div class="tcard"><div class="tcard-hdr"><span style="font-size:11px;font-weight:700;color:#fff">T+5</span>';
  h+='<span style="font-size:11px;font-weight:700;color:'+t5c+'">'+(t5dir.indexOf("多")>=0?"↑":t5dir.indexOf("空")>=0?"↓":"↔")+" "+t5dir+"</span>";
  h+='<span style="font-size:13px;font-weight:800;color:'+t5c+';font-family:var(--mono)">'+t5conf+"%</span></div>";
  h+='<div class="tcard-metrics">';
  h+="预估价: "+(ana.expected_price||"—");
  h+="<br>交割率: "+(ana.expected_delivery_rate||"—")+"%";
  h+="<br>期限结构: "+(contango?"升水":"贴水")+"</div></div></div>";
  h+="</div>";

  /* ── Prediction Scenarios (SPAN Stress) ── */
  h+='<div class="sec"><div class="stitle"><div class="sbar" style="background:var(--purple)"></div>交割率情景压力测试 · '+(pred.contract||selC)+"</div>";
  for(var i=0;i<preds.length;i++){var p=preds[i],isH=p.probability===maxP;
    var dc2=p.direction==="偏多"?"var(--green)":p.direction==="偏空"?"var(--red)":"var(--gold)";
    var db2=p.direction==="偏多"?"rgba(34,197,94,0.12);color:var(--green)":p.direction==="偏空"?"rgba(239,68,68,0.12);color:var(--red)":"rgba(245,166,35,0.12);color:var(--gold)";
    var bc2=isH?"linear-gradient(90deg,#a78bfa,#7c3aed)":"rgba(255,255,255,0.1)";
    h+='<div class="prow'+(isH?" hl":"")+'">';
    h+='<div><div style="font-size:10.5px;font-weight:600;color:#fff">'+p.label+"</div>";
    h+='<div style="font-size:9px;color:var(--txt2)">交割率 '+p.delivery_rate+"</div></div>";
    h+='<div style="font-size:13px;font-weight:700;font-family:var(--mono);color:'+dc2+'">'+p.price+"</div>";
    h+='<div class="pbar-outer"><div class="pbar-fill" style="width:'+(p.probability*100)+"%;background:"+bc2+'"></div></div>';
    h+='<div style="text-align:center;font-family:var(--mono);font-weight:600;font-size:10.5px;color:'+(isH?"var(--purple)":"var(--txt2)")+'">'+(p.probability*100).toFixed(0)+"%</div>";
    h+='<div style="font-size:9px;padding:2px 6px;border-radius:3px;text-align:center;font-weight:600;background:'+db2+'">'+p.direction+"</div></div>";}

  /* ── 算法说明 ── */
  h+='<div class="algo"><b>📊 交割率综合分析框架 (IDAF)</b><br>';
  h+="三因子融合: ";
  h+="<span style='color:var(--cyan);font-weight:600'>① DSCR</span>(可交割供给覆盖率, w=35%) → 挤仓风险评估; ";
  h+="<span style='color:var(--gold);font-weight:600'>② Basis-Carry</span>(基差-持有成本, w=30%) → 交割激励与收敛性; ";
  h+="<span style='color:var(--purple);font-weight:600'>③ SPAN-Stress</span>(情景压力测试, w=20%) → 技术面+资金面冲击。";
  h+="<br><span style='font-size:9.5px;color:var(--dim)'>参考: CFTC Deliverable Supply · Theory of Storage (Kaldor/Working) · CME SPAN Framework";
  var wt=ana.weights||{};
  if(wt.DSCR!==undefined){h+=" | 权重: DSCR="+wt.DSCR+", Basis="+wt["Basis-Carry"]+", Tech="+wt.Technical+", Prior="+wt.Prior;}
  h+="</span>";
  h+="<br>最可能: <b>"+(best.label||"—")+" ("+(best.delivery_rate||"—")+")</b>"
  h+="，概率加权价格 <span style='color:var(--green);font-weight:600'>"+(ana.expected_price||"—")+"</span>。</div>";
  h+="</div>";

  /* ── 多维信号面板 ── */
  h+='<div class="sec"><div class="stitle"><div class="sbar" style="background:var(--green)"></div>多维信号面板</div>';
  // 基于IDAF三模块的信号判断
  var dscrD=dscr<1.5?"偏多":dscr>3?"偏空":"中性"; // DSCR低→供给紧→偏多
  var basisD=incS>0.1?"偏空":incS<-0.1?"偏多":"中性"; // 激励正=利交割=偏空,激励负=不利交割=偏多
  var techD=isUp&&ma5>ma20?"偏多":!isUp&&ma5<ma20?"偏空":"中性";
  var fundD=(ind.inventory_change||0)<0?"偏多":(ind.inventory_change||0)>0?"偏空":"中性";
  var monD=(ind.position_ratio||0)>1?"偏多":"偏空";
  var newsD=sentLabel2.indexOf("利好")>=0?"偏多":sentLabel2.indexOf("利空")>=0?"偏空":"中性";
  function mc3(d){return d==="偏多"?"var(--green)":d==="偏空"?"var(--red)":"var(--gold)";}
  h+='<div class="meters">';
  var ms=[{l:"供给覆盖(DSCR)",v:dscrD},{l:"基差激励",v:basisD},{l:"技术面",v:techD},{l:"基本面",v:fundD},{l:"资金面",v:monD},{l:"舆情(FinBERT)",v:newsD}];
  for(var i=0;i<ms.length;i++){h+='<div class="meter"><div class="meter-l">'+ms[i].l+'</div><div class="meter-v" style="color:'+mc3(ms[i].v)+'">'+ms[i].v+"</div></div>";}
  h+="</div>";

  var tags=[],src=[m.metal_name,"期货","价格","库存","供应","需求","ETF","央行","美联储","交割","持仓","升水","贴水","基差","DSCR","挤仓"];
  for(var i=0;i<news.length;i++){var t=news[i].title||"";
    for(var j=0;j<src.length;j++){if(src[j]&&t.indexOf(src[j])>=0&&tags.indexOf(src[j])<0)tags.push(src[j]);}}
  if(tags.length){h+='<div style="margin-top:10px;font-size:10px;color:var(--txt2);font-weight:600;margin-bottom:5px">关键主题</div><div class="tags">';
    for(var i=0;i<tags.length;i++)h+='<span class="tag">'+tags[i]+"</span>";h+="</div>";}
  h+="</div>";

  document.getElementById("panelR").innerHTML=h;
}

window.addEventListener("resize",function(){var kl=((gm()||{}).kline||{}).data||[];if(kl.length)requestAnimationFrame(function(){try{drawK(kl);}catch(e){}});});

// 确保DOM完全就绪后再渲染
if(document.readyState==="loading"){
  document.addEventListener("DOMContentLoaded",function(){renderAll();});
}else{
  renderAll();
}
</script>
</body>
</html>'''


def build(date_str=None):
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    json_path = os.path.join(DATA_DIR, date_str, "market_data.json")
    if not os.path.exists(json_path):
        print("✗ 找不到数据: " + json_path)
        print("  请先运行: python fetcher.py")
        return
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.loads(f.read())

    # ── 补充情绪分析 + 重算预测 + 交割危机 (兼容旧数据) ──
    try:
        from fetcher import _analyze_news_sentiment, compute_predictions, compute_delivery_crisis, compute_per_contract_crisis
        for mid, mdata in data.get("metals", {}).items():
            # 补充情绪
            news_obj = mdata.get("news", {})
            if "sentiment" not in news_obj and "news" in news_obj:
                news_list = news_obj["news"]
                sentiment = _analyze_news_sentiment(news_list, mid)
                news_obj["sentiment"] = sentiment
                print(f"  [补] {mid} 情绪: {sentiment['confidence']}% {sentiment['label']}")
            # 重算预测 (使用新算法)
            old_pred = mdata.get("predictions", {})
            if old_pred.get("analysis", {}).get("model") != "IDAF" or \
               old_pred.get("analysis", {}).get("confidence") is None:
                result = compute_predictions(
                    mid,
                    mdata.get("contracts", {}),
                    mdata.get("indicators", {}),
                    mdata.get("kline", {}),
                )
                if result.get("predictions"):
                    mdata["predictions"] = result
                    ana = result.get("analysis", {})
                    print(f"  [补] {mid} 预测: {ana.get('confidence')}% {ana.get('overall_direction')}")
            # 补充/重算交割危机概率
            if "delivery_crisis" not in mdata or mdata.get("delivery_crisis", {}).get("probability", 0) == 0:
                crisis = compute_delivery_crisis(
                    mid,
                    mdata.get("contracts", {}),
                    mdata.get("indicators", {}),
                    mdata.get("kline", {}),
                    crisis_data=mdata.get("crisis_data", {}),
                )
                mdata["delivery_crisis"] = crisis
                print(f"  [补] {mid} 交割危机: {crisis.get('probability_pct')}% {crisis.get('level')}")
            # 补充/重算逐合约交割危机
            if "per_contract_crisis" not in mdata or not mdata.get("per_contract_crisis"):
                per_c = compute_per_contract_crisis(
                    mid,
                    mdata.get("contracts", {}),
                    mdata.get("indicators", {}),
                    mdata.get("kline", {}),
                    crisis_data=mdata.get("crisis_data", {}),
                )
                mdata["per_contract_crisis"] = per_c
                codes = [x.get("contract","") for x in per_c]
                probs = [f"{x.get('probability_pct',0):.0f}%" for x in per_c]
                print(f"  [补] {mid} 逐合约危机: {dict(zip(codes, probs))}")
    except ImportError:
        pass

    raw = json.dumps(data, ensure_ascii=False)
    docs_dir = os.path.join(BASE_DIR, "docs")
    os.makedirs(docs_dir, exist_ok=True)
    out = os.path.join(docs_dir, "index.html")
    html = TEMPLATE.replace("%%DATA%%", raw).replace("%%DATE%%", date_str)
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    kb = round(os.path.getsize(out) / 1024)
    print("=" * 52)
    print("  ✅ index.html 已生成!")
    print("  📁 " + out)
    print("  📦 " + str(kb) + " KB")
    print("  📅 数据日期: " + date_str)
    print("  💡 双击 docs/index.html 即可打开!")
    print("=" * 52)


if __name__ == "__main__":
    build(sys.argv[1] if len(sys.argv) > 1 else None)