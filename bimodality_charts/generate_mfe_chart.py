#!/usr/bin/env python3
"""
generate_mfe_chart.py — Genera visualización HTML de bimodalidad MFE
Uso: python generate_mfe_chart.py trade_paths.json [output.html]
"""

import json, sys, os
import numpy as np
from scipy import stats
from sklearn.mixture import GaussianMixture

# ─────────────────────────────────────────────────────────────────
# 1. FUNCIONES DE CÁLCULO
# ─────────────────────────────────────────────────────────────────

def mfe_at_time(path, t):
    """MFE acumulado hasta el tiempo t (segundos)"""
    mfe = 0
    for p in path:
        if p[0] > t:
            break
        mfe = max(mfe, p[2])
    return mfe


def interpolate_pnl(path, t):
    """PnL interpolado linealmente en el tiempo exacto t"""
    prev = None
    for p in path:
        if p[0] == t:
            return p[1]
        if p[0] > t:
            if prev is None:
                return p[1]
            dt = p[0] - prev[0]
            if dt == 0:
                return p[1]
            frac = (t - prev[0]) / dt
            return prev[1] + frac * (p[1] - prev[1])
        prev = (p[0], p[1])
    return path[-1][1] if path else 0


def compute_stats_at_tobs(trades_sorted, branches, pnl_branch, win, t_sec, H_Y):
    """Calcula ΔBIC, Ashman D, correlación y clusters para un T_obs dado"""
    n = len(trades_sorted)
    mfe_t = np.array([mfe_at_time(t['path'], t_sec) for t in trades_sorted])

    # ΔBIC
    X = mfe_t.reshape(-1, 1)
    g1 = GaussianMixture(n_components=1, random_state=42).fit(X)
    g2 = GaussianMixture(n_components=2, random_state=42).fit(X)
    dbic = round(g1.bic(X) - g2.bic(X), 1)

    # Ashman D
    mu1, mu2 = sorted(g2.means_.flatten())
    s1, s2 = sorted(np.sqrt(g2.covariances_.flatten()))
    denom = s1**2 + s2**2
    D = round(np.sqrt(2) * abs(mu1 - mu2) / np.sqrt(denom), 2) if denom > 0 else 0

    # Clusters
    lab = g2.predict(X)
    hi = lab == np.argmax(g2.means_.flatten())
    n_hi = int(hi.sum())
    n_lo = int((~hi).sum())

    # Correlación MFE@t vs exit PnL
    rho, p_val = stats.spearmanr(mfe_t, pnl_branch)

    return {
        'dbic': dbic,
        'D': D,
        'rho': round(rho, 3),
        'p': round(p_val, 4),
        'n_hi': n_hi,
        'n_lo': n_lo,
        'mu_lo': round(mu1 * 100, 2),
        'mu_hi': round(mu2 * 100, 2),
    }


# ─────────────────────────────────────────────────────────────────
# 2. PARÁMETROS MFE-BRANCHING
# ─────────────────────────────────────────────────────────────────

T_OBS_SEC = 240      # 4 minutos
THETA = 0.008        # 0.8%
T_GOOD_SEC = 600     # 10 minutos
T_BAD_SEC = 300      # 5 minutos
SL = -0.05           # -5%
SAMPLE_STEP = 15     # MFE path cada 15 segundos
SAMPLE_MAX = 660     # hasta 11 minutos


# ─────────────────────────────────────────────────────────────────
# 3. PROCESAR DATOS
# ─────────────────────────────────────────────────────────────────

def process_trades(input_path):
    """Lee el JSON y calcula todos los datos necesarios para el gráfico"""
    with open(input_path) as f:
        trades = json.load(f)

    trades_sorted = sorted(trades, key=lambda t: t['entry_time'])
    n = len(trades_sorted)
    times = list(range(0, SAMPLE_MAX + 1, SAMPLE_STEP))

    # MFE paths (porcentaje)
    paths = []
    for t in trades_sorted:
        row = [round(mfe_at_time(t['path'], ts) * 100, 3) for ts in times]
        paths.append(row)

    # MFE@T_obs y clasificación
    mfe4 = [round(mfe_at_time(t['path'], T_OBS_SEC) * 100, 2) for t in trades_sorted]
    branches = [m >= THETA * 100 for m in mfe4]

    # Exit PnL con MFE-Branching (interpolación, con SL)
    exit_pnl = []
    for i, t in enumerate(trades_sorted):
        exit_t = T_GOOD_SEC if branches[i] else T_BAD_SEC
        sl_hit = False
        for p in t['path']:
            if p[0] > exit_t:
                break
            if p[1] <= SL:
                sl_hit = True
                break
        if sl_hit:
            exit_pnl.append(round(SL * 100, 2))
        else:
            exit_pnl.append(round(interpolate_pnl(t['path'], exit_t) * 100, 2))

    # Win/loss
    win = np.array([1 if ep > 0 else 0 for ep in exit_pnl])
    pnl_branch = np.array([ep / 100 for ep in exit_pnl])  # en fracción
    branches_np = np.array([m >= THETA * 100 for m in mfe4])

    # H(Y)
    p_w = win.mean()
    H_Y = -p_w * np.log2(p_w + 1e-10) - (1 - p_w) * np.log2(1 - p_w + 1e-10)

    # Stats por T_obs (para el slider)
    stats_by_tobs = {}
    for t_sec in range(60, SAMPLE_MAX + 1, 30):
        t_min = round(t_sec / 60, 1)
        s = compute_stats_at_tobs(trades_sorted, branches_np, pnl_branch, win, t_sec, H_Y)
        stats_by_tobs[str(t_min)] = s

    return {
        'n': n,
        'times': times,
        'paths': paths,
        'mfe4': mfe4,
        'exit_pnl': exit_pnl,
        'stats': stats_by_tobs,
    }


# ─────────────────────────────────────────────────────────────────
# 4. GENERAR HTML
# ─────────────────────────────────────────────────────────────────

def generate_html(data):
    """Genera el HTML completo del gráfico interactivo"""

    n = data['n']
    times_json = json.dumps(data['times'])
    paths_json = json.dumps(data['paths'])
    mfe4_json = json.dumps(data['mfe4'])
    ep_json = json.dumps(data['exit_pnl'])
    stats_json = json.dumps(data['stats'])
    theta_pct = THETA * 100

    html = f'''<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MFE Bimodality — Ψ-JAM {n} trades</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;padding:24px;background:#fff;color:#222}}
@media(prefers-color-scheme:dark){{body{{background:#1a1a1a;color:#ddd}}}}
.leg{{display:flex;flex-wrap:wrap;gap:12px;font-size:12px;color:#888;margin:4px 0 8px}}
.leg span{{display:flex;align-items:center;gap:4px}}
.dot{{width:10px;height:10px;border-radius:2px}}
.ctrl{{display:flex;align-items:center;gap:12px;margin:12px 0}}
.ctrl label{{font-size:13px;color:#888;min-width:40px}}
.ctrl span{{font-size:14px;font-weight:500;min-width:50px}}
input[type=range]{{flex:1;height:4px;-webkit-appearance:none;background:#ddd;border-radius:2px;outline:none}}
input[type=range]::-webkit-slider-thumb{{-webkit-appearance:none;width:18px;height:18px;border-radius:50%;background:#534AB7;cursor:pointer}}
@media(prefers-color-scheme:dark){{input[type=range]{{background:#444}}}}
h2{{font-size:18px;font-weight:500;margin:0 0 8px}}
.stats{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:8px 0}}
.stat{{background:#f5f5f2;border-radius:8px;padding:10px 12px}}
.stat .lbl{{font-size:11px;color:#888;margin:0}}
.stat .val{{font-size:20px;font-weight:500;margin:2px 0 0}}
.stat .sub{{font-size:11px;color:#888;margin:2px 0 0}}
@media(prefers-color-scheme:dark){{.stat{{background:#2a2a28}}}}
</style>
</head>
<body>
<h2>MFE bimodality — \\u03A8-JAM {n} trades</h2>
<div class="leg">
<span><span class="dot" style="background:#1D9E75"></span>Rama buena (MFE@T &ge; {theta_pct}%)</span>
<span><span class="dot" style="background:#888780"></span>Rama mala (MFE@T &lt; {theta_pct}%)</span>
<span><span class="dot" style="background:#D85A30;width:10px;height:2px"></span>Umbral {theta_pct}%</span>
<span><span class="dot" style="background:#534AB7;width:2px;height:10px"></span>T_obs</span>
</div>
<div class="stats" id="statsPanel">
<div class="stat"><p class="lbl">Bimodalidad (\\u0394BIC)</p><p class="val" id="st_dbic">—</p><p class="sub">requiere &gt; 10</p></div>
<div class="stat"><p class="lbl">Separacion (Ashman D)</p><p class="val" id="st_D">—</p><p class="sub">requiere &gt; 1.5</p></div>
<div class="stat"><p class="lbl">Correlacion MFE vs PnL</p><p class="val" id="st_rho">—</p><p class="sub" id="st_p">—</p></div>
<div class="stat"><p class="lbl">Clusters</p><p class="val" id="st_clust">—</p><p class="sub" id="st_mu">—</p></div>
</div>
<div style="position:relative;width:100%;height:300px"><canvas id="c1"></canvas></div>
<div class="ctrl">
<label>T_obs</label>
<input type="range" min="1" max="10" value="{T_OBS_SEC/60:.1f}" step="0.5" id="tobs">
<span id="tobsV">{T_OBS_SEC/60:.1f} min</span>
</div>
<div style="position:relative;width:100%;height:220px"><canvas id="c2"></canvas></div>
<div style="height:16px"></div>
<div style="position:relative;width:100%;height:260px"><canvas id="c3"></canvas></div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-annotation/3.0.1/chartjs-plugin-annotation.min.js"></script>
<script>
const dk=matchMedia('(prefers-color-scheme:dark)').matches;
const gc=dk?'rgba(255,255,255,0.06)':'rgba(0,0,0,0.04)';
const tc=dk?'rgba(255,255,255,0.4)':'rgba(0,0,0,0.35)';
const TEAL='#1D9E75',GRAY='#888780',CORAL='#D85A30',PURPLE='#534AB7';
const TEAL_A=dk?'rgba(29,158,117,0.25)':'rgba(29,158,117,0.15)';
const GRAY_A=dk?'rgba(136,135,128,0.15)':'rgba(136,135,128,0.1)';

const times={times_json};
const P={paths_json};
const EP={ep_json};
const MFE4={mfe4_json};
const STATS={stats_json};

const TH={theta_pct};
const N=P.length;
const timeMin=times.map(t=>t/60);

function getMfeAtIdx(tradeIdx,tMin){{
let ti=0;
for(let j=0;j<times.length;j++){{if(times[j]/60<=tMin)ti=j;else break;}}
return P[tradeIdx][ti];
}}

const datasets=[];
for(let i=0;i<N;i++){{
const isGood=MFE4[i]>=TH;
datasets.push({{data:timeMin.map((t,j)=>({{x:t,y:P[i][j]}})),borderColor:isGood?TEAL_A:GRAY_A,borderWidth:isGood?1.8:0.8,pointRadius:0,tension:0,order:isGood?1:2}});
}}
const ch1=new Chart(document.getElementById('c1'),{{type:'scatter',data:{{datasets}},
options:{{responsive:true,maintainAspectRatio:false,animation:false,showLine:true,
interaction:{{mode:'nearest',intersect:false}},
plugins:{{legend:{{display:false}},tooltip:{{enabled:false}},
annotation:{{annotations:{{
th:{{type:'line',yMin:TH,yMax:TH,borderColor:CORAL,borderWidth:1.5,borderDash:[4,4]}},
obs:{{type:'line',xMin:{T_OBS_SEC/60},xMax:{T_OBS_SEC/60},borderColor:PURPLE,borderWidth:2,
label:{{display:true,content:'T_obs {T_OBS_SEC/60:.0f}m',position:'start',color:PURPLE,
backgroundColor:dk?'rgba(0,0,0,0.7)':'rgba(255,255,255,0.85)',font:{{size:10}},padding:2}}}}
}}}}}},
scales:{{x:{{type:'linear',min:0,max:11,grid:{{color:gc}},ticks:{{color:tc,font:{{size:10}},callback:v=>Math.round(v)+'m'}}}},
y:{{min:0,max:13,grid:{{color:gc}},ticks:{{color:tc,font:{{size:10}},callback:v=>v+'%'}}}}}}
}}}});

let ch2,ch3;
function updateHist(tMin){{
const vals=[];for(let i=0;i<N;i++)vals.push(getMfeAtIdx(i,tMin));
const bins=[];const step=0.5;
for(let b=0;b<16;b++){{const lo=b*step,hi=lo+step;let cG=0,cB=0;
for(let i=0;i<N;i++){{if(vals[i]>=lo&&vals[i]<hi){{if(MFE4[i]>=TH)cG++;else cB++;}}}}
bins.push({{lo,hi,cG,cB}});}}
const labels=bins.map(b=>b.lo.toFixed(1));
if(ch2)ch2.destroy();
ch2=new Chart(document.getElementById('c2'),{{type:'bar',
data:{{labels,datasets:[
{{label:'Buena',data:bins.map(b=>b.cG),backgroundColor:TEAL,borderRadius:2}},
{{label:'Mala',data:bins.map(b=>b.cB),backgroundColor:GRAY,borderRadius:2}}
]}},
options:{{responsive:true,maintainAspectRatio:false,animation:false,
plugins:{{legend:{{display:false}},
annotation:{{annotations:{{
th:{{type:'line',xMin:TH/step,xMax:TH/step,borderColor:CORAL,borderWidth:2,borderDash:[4,4],
label:{{display:true,content:'\\u03b8={theta_pct}%',position:'start',color:CORAL,
backgroundColor:dk?'rgba(0,0,0,0.7)':'rgba(255,255,255,0.85)',font:{{size:10}},padding:2}}}}
}}}}}},
scales:{{x:{{stacked:true,grid:{{display:false}},ticks:{{color:tc,font:{{size:10}},callback:function(v){{return bins[v]?bins[v].lo.toFixed(1)+'%':''}}}}}},
y:{{stacked:true,grid:{{color:gc}},ticks:{{color:tc,font:{{size:10}}}},title:{{display:true,text:'trades',color:tc,font:{{size:11}}}}}}}}}}}});
ch1.options.plugins.annotation.annotations.obs.xMin=tMin;
ch1.options.plugins.annotation.annotations.obs.xMax=tMin;
ch1.options.plugins.annotation.annotations.obs.label.content='T_obs '+tMin.toFixed(1)+'m';
ch1.update('none');
}}

function updateScatter(tMin){{
const pts=[];for(let i=0;i<N;i++){{const m=getMfeAtIdx(i,tMin);pts.push({{x:m,y:EP[i],good:MFE4[i]>=TH}});}}
if(ch3)ch3.destroy();
ch3=new Chart(document.getElementById('c3'),{{type:'scatter',
data:{{datasets:[
{{data:pts.filter(p=>p.good).map(p=>({{x:p.x,y:p.y}})),backgroundColor:TEAL,pointRadius:4,pointStyle:'circle'}},
{{data:pts.filter(p=>!p.good).map(p=>({{x:p.x,y:p.y}})),backgroundColor:GRAY,pointRadius:3,pointStyle:'triangle'}}
]}},
options:{{responsive:true,maintainAspectRatio:false,animation:false,
plugins:{{legend:{{display:false}},
annotation:{{annotations:{{
th:{{type:'line',xMin:TH,xMax:TH,borderColor:CORAL,borderWidth:1.5,borderDash:[4,4]}},
zero:{{type:'line',yMin:0,yMax:0,borderColor:dk?'rgba(255,255,255,0.15)':'rgba(0,0,0,0.1)',borderWidth:1}}
}}}},
tooltip:{{callbacks:{{label:c=>'MFE@T: '+c.parsed.x.toFixed(1)+'%  Exit: '+(c.parsed.y>=0?'+':'')+c.parsed.y.toFixed(1)+'%'}}}}}},
scales:{{x:{{min:-0.5,max:13,grid:{{color:gc}},ticks:{{color:tc,font:{{size:10}},callback:v=>v+'%'}},
title:{{display:true,text:'MFE @ T_obs',color:tc,font:{{size:11}}}}}},
y:{{min:-12,max:13,grid:{{color:gc}},ticks:{{color:tc,font:{{size:10}},callback:v=>(v>=0?'+':'')+v+'%'}},
title:{{display:true,text:'Exit PnL',color:tc,font:{{size:11}}}}}}}}}}}});
}}

function updateStats(tMin){{
const key=tMin.toFixed(1);
const s=STATS[key];
if(!s)return;
document.getElementById('st_dbic').textContent='+'+s.dbic.toFixed(1);
document.getElementById('st_D').textContent=s.D.toFixed(2);
document.getElementById('st_rho').textContent='\\u03c1 = '+(s.rho>=0?'+':'')+s.rho.toFixed(3);
document.getElementById('st_p').textContent=s.p<0.001?'p < 0.001':'p = '+s.p.toFixed(3);
document.getElementById('st_clust').textContent=s.n_hi+' / '+s.n_lo;
document.getElementById('st_mu').textContent='\\u03bc '+s.mu_hi.toFixed(1)+'% / '+s.mu_lo.toFixed(1)+'%';
document.getElementById('st_dbic').style.color=s.dbic>10?'#1D9E75':'#E24B4A';
document.getElementById('st_D').style.color=s.D>1.5?'#1D9E75':'#E24B4A';
}}

updateHist({T_OBS_SEC/60});updateScatter({T_OBS_SEC/60});updateStats({T_OBS_SEC/60});
document.getElementById('tobs').addEventListener('input',function(){{
const v=parseFloat(this.value);
document.getElementById('tobsV').textContent=v.toFixed(1)+' min';
updateHist(v);updateScatter(v);updateStats(v);
}});
</script>
</body>
</html>'''

    return html


# ─────────────────────────────────────────────────────────────────
# 5. MAIN
# ─────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Uso: python generate_mfe_chart.py trade_paths.json [output.html]")
        print()
        print("  trade_paths.json  — archivo JSON con los trade paths del bot")
        print("  output.html       — archivo de salida (default: mfe_bimodality.html)")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else 'mfe_bimodality.html'

    if not os.path.exists(input_path):
        print(f"Error: no existe '{input_path}'")
        sys.exit(1)

    print(f"Leyendo {input_path}...")
    data = process_trades(input_path)
    print(f"  {data['n']} trades procesados")
    print(f"  {len(data['stats'])} puntos de T_obs calculados")

    print(f"Generando {output_path}...")
    html = generate_html(data)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    size_kb = len(html) / 1024
    print(f"  {size_kb:.1f} KB escritos")
    print(f"\nListo. Abrí {output_path} en el navegador.")


if __name__ == '__main__':
    main()
