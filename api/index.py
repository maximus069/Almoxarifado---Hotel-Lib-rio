from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from apscheduler.schedulers.background import BackgroundScheduler
import os
import io
import json
import pandas as pd
from datetime import datetime, date, timedelta

app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.secret_key = 'almoxarifado_canteiro_2025'


# ══════════════════════════════════════════════════════
#  CONEXÃO AO NEON (PostgreSQL)
# ══════════════════════════════════════════════════════

DATABASE_URL = (
    os.environ.get('DATABASE_URL') or
    "postgresql://neondb_owner:npg_lf78EMTYgoxH@ep-weathered-mud-accdfzy0.sa-east-1.aws.neon.tech/neondb?sslmode=require"
)

@contextmanager
def get_db():
    """Context manager: abre conexão, garante commit/rollback e fecha ao sair."""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ══════════════════════════════════════════════════════
#  INICIALIZAÇÃO DO BANCO
# ══════════════════════════════════════════════════════

def init_db():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS itens (
                id          SERIAL PRIMARY KEY,
                nome        TEXT NOT NULL UNIQUE,
                categoria   TEXT,
                tipo        TEXT,
                unidade     TEXT,
                qtd_atual   NUMERIC DEFAULT 0,
                ca          TEXT,
                nivel_gasto TEXT NOT NULL DEFAULT 'GASTO ELEVADO'
            );
            CREATE TABLE IF NOT EXISTS gastos_diarios (
                id         SERIAL PRIMARY KEY,
                item_id    INTEGER REFERENCES itens(id) ON DELETE CASCADE,
                data       DATE    NOT NULL,
                quantidade NUMERIC NOT NULL DEFAULT 0,
                fechado    BOOLEAN NOT NULL DEFAULT FALSE,
                UNIQUE(item_id, data)
            );
            CREATE TABLE IF NOT EXISTS fechamentos (
                id              SERIAL PRIMARY KEY,
                data_fechamento DATE      NOT NULL DEFAULT CURRENT_DATE,
                executado_em    TIMESTAMP NOT NULL DEFAULT NOW(),
                modo            TEXT      NOT NULL DEFAULT 'manual',
                total_itens     INTEGER   NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS snapshots_planilha (
                id           SERIAL PRIMARY KEY,
                gerado_em    TIMESTAMP NOT NULL DEFAULT NOW(),
                semana_ref   DATE      NOT NULL,
                arquivo_nome TEXT      NOT NULL,
                dados_json   TEXT      NOT NULL
            );
            CREATE TABLE IF NOT EXISTS controle_auto (
                chave TEXT PRIMARY KEY,
                valor TEXT
            );
            CREATE TABLE IF NOT EXISTS retiradas_sem_gasto (
                id           SERIAL PRIMARY KEY,
                item_id      INTEGER NOT NULL REFERENCES itens(id) ON DELETE CASCADE,
                data         DATE    NOT NULL DEFAULT CURRENT_DATE,
                quantidade   NUMERIC NOT NULL DEFAULT 1,
                responsavel  TEXT    NOT NULL,
                registrado_em TIMESTAMP NOT NULL DEFAULT NOW()
            );
        ''')
        # Migração segura para bancos já existentes (não quebra dados antigos):
        # itens cadastrados antes deste update recebem nivel_gasto = GASTO ELEVADO,
        # preservando exatamente o comportamento de previsão que já tinham.
        cur.execute('''
            ALTER TABLE itens ADD COLUMN IF NOT EXISTS ca TEXT;
            ALTER TABLE itens ADD COLUMN IF NOT EXISTS nivel_gasto TEXT NOT NULL DEFAULT 'GASTO ELEVADO';
        ''')

init_db()


# ══════════════════════════════════════════════════════
#  NÍVEIS DE GASTO — classificação e fatores de atenuação
#
#  Cada nível pondera o quanto a média de consumo projetada
#  realmente pressiona o estoque na previsão (calcular_previsao).
#  GASTO ELEVADO  → fator 1.0  (comportamento original, sem mudança)
#  GASTO MODERADO → fator 0.7  (suaviza a curva, mais dias de margem)
#  GASTO BAIXO    → fator 0.4  (efeito bem mais conservador)
#  SEM GASTO      → não entra na equação (ver calcular_previsao)
# ══════════════════════════════════════════════════════

NIVEIS_GASTO = ('GASTO ELEVADO', 'GASTO MODERADO', 'GASTO BAIXO', 'SEM GASTO')

FATOR_ATENUACAO = {
    'GASTO ELEVADO':  1.0,
    'GASTO MODERADO': 0.7,
    'GASTO BAIXO':    0.4,
}


def normaliza_nivel(valor):
    """Garante que o nível de gasto seja sempre um dos valores válidos."""
    v = (valor or '').strip().upper()
    return v if v in NIVEIS_GASTO else 'GASTO ELEVADO'


def _qtd_disponivel(cur, item_id, qtd_atual):
    """
    Para itens SEM GASTO: retorna qtd_atual menos a soma das retiradas em aberto.
    A quantidade "disponível" nunca é persistida — é sempre calculada na hora,
    pois a devolução é feita simplesmente apagando a linha da retirada.
    """
    cur.execute(
        'SELECT COALESCE(SUM(quantidade), 0) FROM retiradas_sem_gasto WHERE item_id = %s',
        (item_id,)
    )
    total_ret = float(cur.fetchone()[0])
    return max(0.0, float(qtd_atual or 0) - total_ret)


# ══════════════════════════════════════════════════════
#  FECHAMENTO AUTOMÁTICO — toda sexta às 17h
# ══════════════════════════════════════════════════════

def fechamento_automatico():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            _executar_fechamento(cur, modo='automatico')
    except Exception as e:
        print(f"[scheduler] Erro no fechamento automático: {e}")

scheduler = BackgroundScheduler()
scheduler.add_job(fechamento_automatico, 'cron', day_of_week='fri', hour=17, minute=0)
scheduler.start()


# ══════════════════════════════════════════════════════
#  GATILHO EXTERNO DE CRON — geração da planilha
#
#  Use esta rota como alternativa ao APScheduler quando
#  o servidor não mantém processo contínuo (ex: Render
#  Free, Railway, Vercel). Configure um cron externo
#  (cron-job.org, EasyCron, GitHub Actions, etc.) para
#  fazer GET nesta URL toda sexta às 17h no horário
#  desejado:
#
#    GET https://<seu-dominio>/api/disparar-tarefa
#
#  A rota chama exatamente a mesma função do scheduler
#  interno, garantindo comportamento idêntico.
#  Proteja com uma variável de ambiente CRON_SECRET se
#  a URL for pública, comparando o header Authorization.
# ══════════════════════════════════════════════════════

@app.route('/api/disparar-tarefa', methods=['GET'])
def disparar_tarefa():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            _executar_fechamento(cur, modo='automatico')
        return "Cron executado com sucesso!", 200
    except Exception as e:
        return f"Erro ao executar cron: {e}", 500


# ══════════════════════════════════════════════════════
#  ENGINE DE PREVISÃO (regressão linear OLS)
# ══════════════════════════════════════════════════════

def calcular_previsao(gastos_lista, qtd_atual, nivel_gasto='GASTO ELEVADO'):
    """
    gastos_lista : lista de tuplas (data, quantidade) — até 30 dias
    qtd_atual    : estoque atual do item
    nivel_gasto  : classificação cadastrada no item — pondera o quanto
                   o consumo projetado pressiona a previsão (ver
                   FATOR_ATENUACAO). 'SEM GASTO' não entra na equação.

    • 1–2 amostras → média simples
    • 3+ amostras  → OLS (captura tendência de alta/baixa)
    • Cenário pessimista = projeção + 0.5 × desvio
    • Confiança cresce com mais amostras e menor variabilidade
    """
    default = {
        "status": "Sem dados", "classe": "risco-nulo",
        "dias_restantes": None, "media_diaria": 0,
        "desvio_padrao": 0, "confianca": 0, "tendencia": 0
    }

    nivel = normaliza_nivel(nivel_gasto)
    if nivel == 'SEM GASTO':
        # Item não passa pela equação de previsão por enquanto — fica
        # equivalente a "sem histórico", mesmo que tenha qtd_atual.
        return {**default, "status": "Estável", "classe": "risco-nulo"}

    if not gastos_lista or qtd_atual is None:
        return default

    qtd         = float(qtd_atual)
    quantidades = [float(g[1]) for g in gastos_lista]
    n           = len(quantidades)

    if n == 0 or all(q == 0 for q in quantidades):
        return {**default, "status": "Estável", "classe": "risco-nulo"}

    media  = sum(quantidades) / n
    desvio = 0
    if n > 1:
        desvio = (sum((q - media) ** 2 for q in quantidades) / (n - 1)) ** 0.5

    tendencia_b = 0
    media_proj  = media

    if n >= 3:
        x   = list(range(n))
        xm  = sum(x) / n
        num = sum((x[i] - xm) * (quantidades[i] - media) for i in range(n))
        den = sum((xi - xm) ** 2 for xi in x)
        if den != 0:
            tendencia_b = num / den
            a           = media - tendencia_b * xm
            proj_prox   = max(0.0, a + tendencia_b * n)
            peso        = min(1.0, n / 20) * 0.45
            media_proj  = media * (1 - peso) + proj_prox * peso

    # Atenua a projeção de consumo conforme o nível cadastrado do item.
    # GASTO ELEVADO mantém fator 1.0 (comportamento idêntico ao original).
    media_proj *= FATOR_ATENUACAO.get(nivel, 1.0)

    taxa_pess = (media_proj + 0.5 * desvio) if media_proj > 0 else media_proj
    cv        = (desvio / media) if media > 0 else 1.0
    confianca = max(5, min(100, int((n / 20) * 100 * max(0, 1 - min(cv, 1)))))

    if media_proj <= 0:
        return {**default, "status": "Estável", "classe": "risco-nulo",
                "media_diaria": round(media, 2), "desvio_padrao": round(desvio, 2),
                "confianca": confianca, "tendencia": round(tendencia_b, 3)}

    dias_central = qtd / media_proj

    if dias_central <= 7:
        status, classe = "CRÍTICO", "risco-critico"
    elif dias_central <= 14:
        status, classe = f"⚠ {int(dias_central)}d", "risco-proximo"
    else:
        status, classe = f"{int(dias_central)} dias", "risco-ok"

    return {
        "status":         status,
        "classe":         classe,
        "dias_restantes": round(dias_central, 1),
        "media_diaria":   round(media, 2),
        "desvio_padrao":  round(desvio, 2),
        "confianca":      confianca,
        "tendencia":      round(tendencia_b, 3),
    }


# ══════════════════════════════════════════════════════
#  FECHAMENTO DE SEMANA (lógica central)
# ══════════════════════════════════════════════════════

def _executar_fechamento(cur, modo='manual'):
    """
    1. Soma gastos NÃO fechados → desconta do estoque.
    2. Marca esses registros como fechado=TRUE (mantidos 30 dias para previsão).
    3. Remove registros com mais de 30 dias.
    4. Gera snapshot (planilha) com qtd_atual pós-fechamento e Qtd. Gasta zerada.
    5. Registra o fechamento na tabela fechamentos.
    Retorna (n_itens, dados_snapshot, nome_arquivo).
    """
    # Desconta gastos abertos do estoque — exclui itens SEM GASTO (patrimônio):
    # ferramentas não têm "gasto" de consumo, apenas retiradas rastreadas separadamente.
    cur.execute('''
        SELECT gd.item_id, COALESCE(SUM(gd.quantidade), 0)
        FROM gastos_diarios gd
        JOIN itens i ON i.id = gd.item_id
        WHERE gd.fechado = FALSE
          AND UPPER(i.nivel_gasto) != 'SEM GASTO'
        GROUP BY gd.item_id
    ''')
    for item_id, total in cur.fetchall():
        if float(total) > 0:
            cur.execute('''
                UPDATE itens SET qtd_atual = GREATEST(0, qtd_atual - %s) WHERE id = %s
            ''', (float(total), item_id))

    # Fecha gastos e limpa histórico antigo
    cur.execute("UPDATE gastos_diarios SET fechado = TRUE WHERE fechado = FALSE")
    cur.execute("DELETE FROM gastos_diarios WHERE data < %s", (date.today() - timedelta(days=30),))

    # Gera snapshot — SEM GASTO sempre com Qtd. Gasta = 0
    cur.execute('SELECT nome, ca, nivel_gasto, categoria, tipo, unidade, qtd_atual FROM itens ORDER BY nome ASC')
    rows = cur.fetchall()
    dados_snapshot = [
        {
            "Item": r[0], "CA": r[1] or '', "Nivel": r[2],
            "Categoria": r[3], "Tipo": r[4], "Unid.": r[5],
            "Qtd. Inicial": float(r[6] or 0),
            # SEM GASTO → Qtd. Gasta sempre 0; patrimônio não se "consome"
            "Qtd. Gasta": 0.0
        }
        for r in rows
    ]

    semana_ref   = date.today()
    arquivo_nome = f"Almoxarifado_{semana_ref.strftime('%Y-%m-%d')}.xlsx"

    cur.execute('''
        INSERT INTO snapshots_planilha (gerado_em, semana_ref, arquivo_nome, dados_json)
        VALUES (NOW(), %s, %s, %s)
    ''', (semana_ref, arquivo_nome, json.dumps(dados_snapshot)))

    cur.execute('''
        INSERT INTO fechamentos (data_fechamento, modo, total_itens)
        VALUES (CURRENT_DATE, %s, %s)
    ''', (modo, len(rows)))

    return len(rows), dados_snapshot, arquivo_nome


# ══════════════════════════════════════════════════════
#  HOME — lista de itens (sem filtros de backend;
#  filtros são 100% locais no JS do frontend)
# ══════════════════════════════════════════════════════

@app.route('/', methods=['GET'])
def index():
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Busca todos os itens de uma vez
        cur.execute('SELECT id, nome, ca, nivel_gasto, categoria, tipo, unidade, qtd_atual FROM itens ORDER BY nome ASC')
        rows = cur.fetchall()

        # Busca todos os gastos ativos de uma vez (evita N+1 queries)
        ids = [r['id'] for r in rows]
        gastos_por_item = {i: [] for i in ids}
        if ids:
            cur.execute('''
                SELECT item_id, data, quantidade
                FROM gastos_diarios
                WHERE item_id = ANY(%s) AND fechado = FALSE
                ORDER BY data DESC
            ''', (ids,))
            for g in cur.fetchall():
                gastos_por_item[g['item_id']].append((g['data'], g['quantidade']))

        # Soma de retiradas em aberto por item SEM GASTO (lote, sem N+1)
        retiradas_por_item = {i: 0.0 for i in ids}
        if ids:
            cur.execute('''
                SELECT item_id, COALESCE(SUM(quantidade), 0)
                FROM retiradas_sem_gasto
                WHERE item_id = ANY(%s)
                GROUP BY item_id
            ''', (ids,))
            for item_id, total in cur.fetchall():
                retiradas_por_item[item_id] = float(total)

        # Monta lista de itens com previsão
        itens = []
        criticos = alertas = 0
        for row in rows:
            nivel    = normaliza_nivel(row['nivel_gasto'])
            qtd_base = float(row['qtd_atual'] or 0)
            # Para SEM GASTO, a quantidade exibida é a disponível (descontando retiradas)
            qtd_exibir = (max(0.0, qtd_base - retiradas_por_item[row['id']])
                          if nivel == 'SEM GASTO' else qtd_base)
            prev  = calcular_previsao(gastos_por_item[row['id']], qtd_base, nivel)
            classe = prev['classe']
            if classe == 'risco-critico': criticos += 1
            elif classe == 'risco-proximo': alertas += 1
            itens.append({
                'id':          row['id'],
                'nome':        row['nome'],
                'ca':          row['ca'] or '',
                'nivel_gasto': nivel,
                'categoria':   row['categoria'] or '—',
                'tipo':        row['tipo'] or '—',
                'qtd_atual':   qtd_exibir,
                'unidade':     row['unidade'] or '',
                'prev_status': prev['status'],
                'classe_risco': classe,
            })

        # Próxima sexta às 17h
        hoje = date.today()
        dias = (4 - hoje.weekday()) % 7 or 7
        proxima_sexta = (hoje + timedelta(days=dias)).strftime('%d/%m/%Y')

        # Último snapshot gerado
        cur.execute('''
            SELECT id, arquivo_nome, gerado_em FROM snapshots_planilha
            ORDER BY gerado_em DESC LIMIT 1
        ''')
        snap_row = cur.fetchone()
        snapshot = {
            'id': snap_row['id'],
            'arquivo_nome': snap_row['arquivo_nome'],
            'gerado_em_fmt': snap_row['gerado_em'].strftime('%d/%m/%Y %H:%M')
        } if snap_row else None

    return render_template('index.html',
        itens=itens,
        total_itens=len(itens),
        criticos=criticos,
        alertas=alertas,
        proximo_domingo=proxima_sexta,
        snapshot=snapshot,
        hoje=str(hoje))


# ══════════════════════════════════════════════════════
#  ADICIONAR ITEM — retorna JSON para o frontend
# ══════════════════════════════════════════════════════

@app.route('/adicionar', methods=['POST'])
def adicionar_item():
    nome  = request.form.get('nome', '').strip().upper()
    ca    = request.form.get('ca', '').strip()           # opcional — pode ficar vazio
    nivel = normaliza_nivel(request.form.get('nivel_gasto', ''))
    cat   = request.form.get('categoria', '').strip()
    tipo  = request.form.get('tipo', '').strip()
    unid  = request.form.get('unidade', '').strip()
    qtd   = float(request.form.get('qtd', 0) or 0)

    if not nome or not cat or not tipo or not unid:
        return jsonify({'ok': False, 'erro': 'Preencha todos os campos.'}), 400

    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO itens (nome, ca, nivel_gasto, categoria, tipo, unidade, qtd_atual)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (nome) DO UPDATE SET
                    ca          = EXCLUDED.ca,
                    nivel_gasto = EXCLUDED.nivel_gasto,
                    categoria   = EXCLUDED.categoria,
                    tipo        = EXCLUDED.tipo,
                    unidade     = EXCLUDED.unidade,
                    qtd_atual   = EXCLUDED.qtd_atual
                RETURNING id
            ''', (nome, ca or None, nivel, cat, tipo, unid, qtd))
            novo_id = cur.fetchone()[0]

        return jsonify({'ok': True, 'item': {
            'id': novo_id, 'nome': nome, 'ca': ca, 'nivel_gasto': nivel,
            'categoria': cat, 'tipo': tipo, 'unidade': unid, 'qtd_atual': qtd,
            'classe_risco': 'risco-nulo', 'prev_status': 'Sem dados'
        }})
    except Exception as e:
        return jsonify({'ok': False, 'erro': str(e)}), 500


# ══════════════════════════════════════════════════════
#  DELETAR ITEM — retorna JSON para o frontend
# ══════════════════════════════════════════════════════

@app.route('/deletar/<int:item_id>')
def deletar_item(item_id):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM itens WHERE id = %s", (item_id,))
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'erro': str(e)}), 500


# ══════════════════════════════════════════════════════
#  REGISTRAR GASTO — retorna JSON com nova qtd e previsão
# ══════════════════════════════════════════════════════

@app.route('/registrar_gasto', methods=['POST'])
def registrar_gasto():
    item_id = request.form.get('item_id')
    qtd     = float(request.form.get('quantidade', 0) or 0)
    try:
        data_gasto = datetime.strptime(request.form.get('data_gasto', ''), '%Y-%m-%d').date()
    except ValueError:
        data_gasto = date.today()

    if not item_id or qtd <= 0:
        return jsonify({'ok': False, 'erro': 'Dados inválidos.'}), 400

    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute('''
                INSERT INTO gastos_diarios (item_id, data, quantidade)
                VALUES (%s, %s, %s)
                ON CONFLICT (item_id, data) DO UPDATE SET quantidade = EXCLUDED.quantidade
            ''', (int(item_id), data_gasto, qtd))

            # Retorna qtd atual e previsão recalculada para o frontend atualizar sem reload
            cur.execute('SELECT qtd_atual, nivel_gasto FROM itens WHERE id = %s', (int(item_id),))
            item_row = cur.fetchone()
            qtd_atual = float(item_row['qtd_atual'] or 0)
            nivel     = normaliza_nivel(item_row['nivel_gasto'])

            cur.execute('''
                SELECT data, quantidade FROM gastos_diarios
                WHERE item_id = %s AND fechado = FALSE ORDER BY data DESC LIMIT 30
            ''', (int(item_id),))
            gastos = [(r['data'], r['quantidade']) for r in cur.fetchall()]

        prev = calcular_previsao(gastos, qtd_atual, nivel)
        return jsonify({
            'ok':          True,
            'qtd_atual':   qtd_atual,
            'classe_risco': prev['classe'],
            'prev_status': prev['status'],
        })
    except Exception as e:
        return jsonify({'ok': False, 'erro': str(e)}), 500


# ══════════════════════════════════════════════════════
#  FECHAR SEMANA — POST com redirect (ação destrutiva)
# ══════════════════════════════════════════════════════

@app.route('/fechar_semana_todos', methods=['POST'])
def fechar_semana_todos():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            n_itens, _, nome_arquivo = _executar_fechamento(cur, modo='manual')
        flash(f"✅ Semana fechada! {n_itens} item(ns) processado(s). Planilha: {nome_arquivo}")
    except Exception as e:
        flash(f"Erro ao fechar semana: {e}")
    return redirect(url_for('index'))


# ══════════════════════════════════════════════════════
#  PLANILHAS — download de snapshot gerado
# ══════════════════════════════════════════════════════

@app.route('/download_planilha/<int:snap_id>')
def download_planilha(snap_id):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute('SELECT dados_json, arquivo_nome FROM snapshots_planilha WHERE id = %s', (snap_id,))
        snap = cur.fetchone()

    if not snap:
        flash("Planilha não encontrada.")
        return redirect(url_for('index'))

    df     = pd.DataFrame(json.loads(snap['dados_json']),
                          columns=["Item", "CA", "Nivel", "Categoria", "Tipo", "Unid.", "Qtd. Inicial", "Qtd. Gasta"])
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Almoxarifado')
        ws = writer.sheets['Almoxarifado']
        for col in ws.columns:
            w = max((len(str(c.value)) for c in col if c.value), default=8)
            ws.column_dimensions[col[0].column_letter].width = min(w + 4, 40)
    output.seek(0)

    return send_file(output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=snap['arquivo_nome'])


# ══════════════════════════════════════════════════════
#  UPLOAD EXCEL — sincroniza planilha com detecção
#  de divergências de quantidade
# ══════════════════════════════════════════════════════

@app.route('/upload_excel', methods=['POST'])
def upload_excel():
    file = request.files.get('file')
    if not file:
        flash("Nenhum arquivo enviado.")
        return redirect(url_for('index'))

    try:
        data_gasto = datetime.strptime(
            request.form.get('data_gasto', ''), '%Y-%m-%d'
        ).date()
    except ValueError:
        data_gasto = date.today()

    def safe_str(row, col):
        v = row.get(col, '')
        return '' if str(v).lower() == 'nan' else str(v).strip()

    def safe_num(row, col):
        try:
            v = row.get(col, 0)
            return float(v) if str(v).lower() not in ('nan', '') else 0.0
        except Exception:
            return 0.0

    try:
        df = pd.read_excel(file)
        df.columns = [str(c).strip() for c in df.columns]

        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Carrega estoque atual para comparar divergências
            cur.execute('SELECT nome, qtd_atual, unidade FROM itens')
            sistema = {r['nome'].upper(): dict(r) for r in cur.fetchall()}

            divergencias  = []
            itens_sync    = 0

            for _, row in df.iterrows():
                nome = str(row.get('Item', row.get('item', ''))).strip().upper()
                if not nome or nome.lower() == 'nan':
                    continue

                ca          = safe_str(row, 'CA')          # opcional — pode vir vazio
                nivel       = normaliza_nivel(safe_str(row, 'Nivel'))
                cat         = safe_str(row, 'Categoria')
                tipo        = safe_str(row, 'Tipo')
                unidade     = safe_str(row, 'Unid.')
                qtd_inicial = safe_num(row, 'Qtd. Inicial')
                qtd_gasta   = safe_num(row, 'Qtd. Gasta')

                # Detecta divergência de quantidade com o sistema
                if nome in sistema:
                    qtd_sis = float(sistema[nome]['qtd_atual'] or 0)
                    if abs(qtd_inicial - qtd_sis) > 0.01:
                        divergencias.append({
                            'nome':        nome,
                            'qtd_planilha': qtd_inicial,
                            'qtd_sistema':  qtd_sis,
                            'diferenca':    round(qtd_inicial - qtd_sis, 2),
                            'unidade':      unidade or sistema[nome].get('unidade', '')
                        })
                        # Atualiza apenas campos de cadastro, não a quantidade
                        cur.execute(
                            'UPDATE itens SET ca=%s, nivel_gasto=%s, categoria=%s, tipo=%s, unidade=%s WHERE UPPER(nome)=%s',
                            (ca or None, nivel, cat, tipo, unidade, nome)
                        )
                        itens_sync += 1
                        continue

                # Sem divergência: upsert completo
                cur.execute('''
                    INSERT INTO itens (nome, ca, nivel_gasto, categoria, tipo, unidade, qtd_atual)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nome) DO UPDATE SET
                        ca          = EXCLUDED.ca,
                        nivel_gasto = EXCLUDED.nivel_gasto,
                        categoria   = EXCLUDED.categoria,
                        tipo        = EXCLUDED.tipo,
                        unidade     = EXCLUDED.unidade,
                        qtd_atual   = EXCLUDED.qtd_atual
                ''', (nome, ca or None, nivel, cat, tipo, unidade, qtd_inicial))
                itens_sync += 1

                # Registra gasto se houver
                if qtd_gasta > 0:
                    cur.execute('SELECT id FROM itens WHERE UPPER(nome) = %s', (nome,))
                    item_row = cur.fetchone()
                    if item_row:
                        cur.execute('''
                            INSERT INTO gastos_diarios (item_id, data, quantidade)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (item_id, data) DO UPDATE SET quantidade = EXCLUDED.quantidade
                        ''', (item_row['id'], data_gasto, qtd_gasta))

        if divergencias:
            # json.dumps com ensure_ascii=True garante que nenhum caractere especial
            # quebre o HTML gerado pelo Jinja ao interpolar o JSON dentro do onclick.
            # separators sem espaços evita qualquer ambiguidade no split('||').
            divs_json = json.dumps(divergencias, ensure_ascii=True, separators=(',', ':'))
            flash(f"DIVERGENCIA:{divs_json}||{len(divergencias)} item(ns) com divergencia de quantidade")
        else:
            flash(f"Planilha importada! {itens_sync} item(ns) sincronizado(s).")

    except Exception as e:
        flash(f"Erro ao importar planilha: {e}")

    return redirect(url_for('index'))


# ══════════════════════════════════════════════════════
#  ACEITAR DIVERGÊNCIA — sobrescreve qtd no sistema
# ══════════════════════════════════════════════════════

@app.route('/aceitar_divergencia', methods=['POST'])
def aceitar_divergencia():
    nome     = (request.form.get('nome') or '').strip()
    nova_qtd = request.form.get('nova_qtd')
    if not nome or nova_qtd is None:
        return jsonify({'ok': False, 'erro': 'Dados ausentes.'}), 400
    try:
        nova_qtd_float = float(nova_qtd)
    except (ValueError, TypeError):
        return jsonify({'ok': False, 'erro': 'Quantidade invalida.'}), 400
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute('UPDATE itens SET qtd_atual = %s WHERE UPPER(nome) = %s',
                        (nova_qtd_float, nome.upper()))
            if cur.rowcount == 0:
                return jsonify({'ok': False, 'erro': 'Item nao encontrado.'}), 404
        return jsonify({'ok': True, 'nome': nome, 'nova_qtd': nova_qtd_float})
    except Exception as e:
        return jsonify({'ok': False, 'erro': str(e)}), 500


# ══════════════════════════════════════════════════════
#  API — detalhes de um item (modal de detalhes)
# ══════════════════════════════════════════════════════

@app.route('/api/item/<int:item_id>')
def api_item(item_id):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute('SELECT * FROM itens WHERE id = %s', (item_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({'error': 'not found'}), 404
        item  = dict(row)
        nivel = normaliza_nivel(item.get('nivel_gasto'))

        cur.execute('''
            SELECT data, quantidade, fechado FROM gastos_diarios
            WHERE item_id = %s ORDER BY data ASC
        ''', (item_id,))
        gastos_raw = cur.fetchall()

        # Retiradas só existem para itens SEM GASTO
        retiradas = []
        if nivel == 'SEM GASTO':
            cur.execute('''
                SELECT id, data, quantidade, responsavel
                FROM retiradas_sem_gasto
                WHERE item_id = %s ORDER BY data DESC, registrado_em DESC
            ''', (item_id,))
            retiradas = [
                {'id': r['id'], 'data': str(r['data']),
                 'quantidade': float(r['quantidade']),
                 'responsavel': r['responsavel']}
                for r in cur.fetchall()
            ]

    prev = calcular_previsao(
        [(g['data'], g['quantidade']) for g in gastos_raw],
        item['qtd_atual'],
        nivel
    )
    return jsonify({
        'item':      {k: str(v) if v is not None else '' for k, v in item.items()},
        'gastos':    [{'data': str(g['data']), 'quantidade': float(g['quantidade']),
                       'fechado': g['fechado']} for g in gastos_raw],
        'retiradas': retiradas,
        'previsao':  prev,
    })


# ══════════════════════════════════════════════════════
#  ATUALIZAR QUANTIDADE TOTAL — modal de detalhes
#  (funciona para qualquer nível; para SEM GASTO altera
#   o total bruto da empresa, não a quantidade disponível)
# ══════════════════════════════════════════════════════

@app.route('/api/item/<int:item_id>/atualizar_qtd', methods=['POST'])
def atualizar_qtd(item_id):
    try:
        nova_qtd = float(request.form.get('nova_qtd', 0) or 0)
    except (ValueError, TypeError):
        return jsonify({'ok': False, 'erro': 'Quantidade inválida.'}), 400
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute('UPDATE itens SET qtd_atual = %s WHERE id = %s RETURNING nivel_gasto',
                        (nova_qtd, item_id))
            row = cur.fetchone()
            if not row:
                return jsonify({'ok': False, 'erro': 'Item não encontrado.'}), 404
            nivel = normaliza_nivel(row['nivel_gasto'])
            # Para SEM GASTO retorna o disponível (total − retiradas)
            qtd_disp = _qtd_disponivel(cur, item_id, nova_qtd) if nivel == 'SEM GASTO' else nova_qtd
        return jsonify({'ok': True, 'qtd_disponivel': qtd_disp})
    except Exception as e:
        return jsonify({'ok': False, 'erro': str(e)}), 500


# ══════════════════════════════════════════════════════
#  RETIRADAS — registro e devolução de itens SEM GASTO
# ══════════════════════════════════════════════════════

@app.route('/api/retirada', methods=['POST'])
def registrar_retirada():
    item_id     = request.form.get('item_id')
    responsavel = (request.form.get('responsavel') or '').strip()
    try:
        data_ret = datetime.strptime(request.form.get('data', ''), '%Y-%m-%d').date()
    except ValueError:
        data_ret = date.today()
    try:
        qtd = float(request.form.get('quantidade', 0) or 0)
    except (ValueError, TypeError):
        qtd = 0.0

    if not item_id or qtd <= 0:
        return jsonify({'ok': False, 'erro': 'Dados inválidos.'}), 400
    if not responsavel:
        return jsonify({'ok': False, 'erro': 'Informe o responsável.'}), 400

    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            # Confirma que o item é SEM GASTO antes de inserir
            cur.execute('SELECT qtd_atual, nivel_gasto FROM itens WHERE id = %s', (int(item_id),))
            row = cur.fetchone()
            if not row or normaliza_nivel(row['nivel_gasto']) != 'SEM GASTO':
                return jsonify({'ok': False, 'erro': 'Item não é SEM GASTO.'}), 400

            cur.execute('''
                INSERT INTO retiradas_sem_gasto (item_id, data, quantidade, responsavel)
                VALUES (%s, %s, %s, %s) RETURNING id
            ''', (int(item_id), data_ret, qtd, responsavel))
            nova_id  = cur.fetchone()[0]
            qtd_disp = _qtd_disponivel(cur, int(item_id), row['qtd_atual'])

        return jsonify({
            'ok': True,
            'retirada_id':   nova_id,
            'qtd_disponivel': qtd_disp,
        })
    except Exception as e:
        return jsonify({'ok': False, 'erro': str(e)}), 500


@app.route('/api/retirada/<int:retirada_id>', methods=['DELETE'])
def excluir_retirada(retirada_id):
    try:
        with get_db() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            # Busca o item_id antes de deletar para calcular disponível após
            cur.execute('SELECT item_id FROM retiradas_sem_gasto WHERE id = %s', (retirada_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({'ok': False, 'erro': 'Retirada não encontrada.'}), 404
            item_id = row['item_id']

            cur.execute('DELETE FROM retiradas_sem_gasto WHERE id = %s', (retirada_id,))

            cur.execute('SELECT qtd_atual FROM itens WHERE id = %s', (item_id,))
            qtd_atual = float(cur.fetchone()['qtd_atual'] or 0)
            qtd_disp  = _qtd_disponivel(cur, item_id, qtd_atual)

        return jsonify({'ok': True, 'qtd_disponivel': qtd_disp})
    except Exception as e:
        return jsonify({'ok': False, 'erro': str(e)}), 500


# ══════════════════════════════════════════════════════
#  API — lista de snapshots (histórico de planilhas)
# ══════════════════════════════════════════════════════

@app.route('/api/snapshots')
def api_snapshots():
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute('''
            SELECT id, arquivo_nome, gerado_em, semana_ref
            FROM snapshots_planilha ORDER BY gerado_em DESC LIMIT 10
        ''')
        snaps = [{'id': r['id'], 'arquivo_nome': r['arquivo_nome'],
                  'gerado_em': r['gerado_em'].strftime('%d/%m/%Y %H:%M'),
                  'semana_ref': str(r['semana_ref'])} for r in cur.fetchall()]
    return jsonify(snaps)


if __name__ == '__main__':
    app.run(debug=True)
