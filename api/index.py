from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import psycopg2
import psycopg2.extras
import os
import pandas as pd
from datetime import datetime, date

app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.secret_key = 'almoxarifado_canteiro_2025'

# ─── Conexão DB ────────────────────────────────────────────────────────────────
def get_db_connection():
    url = os.environ.get('DATABASE_URL') or \
          "postgresql://neondb_owner:npg_lf78EMTYgoxH@ep-weathered-mud-accdfzy0.sa-east-1.aws.neon.tech/neondb?sslmode=require"
    return psycopg2.connect(url)

# ─── Inicialização do banco ────────────────────────────────────────────────────
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    # Tabela principal de itens
    cur.execute('''
        CREATE TABLE IF NOT EXISTS itens (
            id        SERIAL PRIMARY KEY,
            nome      TEXT NOT NULL UNIQUE,
            categoria TEXT,
            tipo      TEXT,
            unidade   TEXT,
            qtd_atual NUMERIC DEFAULT 0
        );
    ''')

    # Tabela de gastos diários (máx 6 por item — 1 semana de trabalho)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS gastos_diarios (
            id         SERIAL PRIMARY KEY,
            item_id    INTEGER REFERENCES itens(id) ON DELETE CASCADE,
            data       DATE NOT NULL,
            quantidade NUMERIC NOT NULL DEFAULT 0,
            UNIQUE(item_id, data)
        );
    ''')

    conn.commit()
    cur.close()
    conn.close()

init_db()

# ─── Engine de Previsão ────────────────────────────────────────────────────────
def calcular_previsao(gastos_lista, qtd_atual):
    """
    gastos_lista: lista de tuplas (data, quantidade)
    qtd_atual:    estoque atual do item

    Lógica:
      - Com 1-2 pontos: média simples
      - Com 3+ pontos:  regressão linear (OLS) para detectar tendência
      - Cenário pessimista: usa média + 0.5 * desvio_padrão
      - Confiança sobe com nº de amostras e consistência dos dados
    """
    default = {
        "status": "Sem dados", "classe": "risco-nulo",
        "dias_restantes": None, "media_diaria": 0,
        "desvio_padrao": 0, "confianca": 0, "tendencia": 0
    }

    if not gastos_lista or qtd_atual is None:
        return default

    qtd = float(qtd_atual)
    quantidades = [float(g[1]) for g in gastos_lista]
    n = len(quantidades)

    if n == 0 or all(q == 0 for q in quantidades):
        return {**default, "status": "Estável", "classe": "risco-nulo"}

    # Média simples
    media = sum(quantidades) / n

    # Desvio padrão amostral
    desvio = 0
    if n > 1:
        variancia = sum((q - media) ** 2 for q in quantidades) / (n - 1)
        desvio = variancia ** 0.5

    # Regressão linear (OLS) com 3+ pontos
    tendencia_b = 0
    media_proj  = media

    if n >= 3:
        x = list(range(n))
        xm = sum(x) / n
        ym = media
        num = sum((x[i] - xm) * (quantidades[i] - ym) for i in range(n))
        den = sum((xi - xm) ** 2 for xi in x)

        if den != 0:
            tendencia_b = num / den                     # inclinação
            a = ym - tendencia_b * xm
            proj_prox   = max(0.0, a + tendencia_b * n) # próximo dia previsto

            # Blend: quanto mais dados, mais peso na projeção de tendência
            peso = min(1.0, n / 6) * 0.45
            media_proj = media * (1 - peso) + proj_prox * peso

    # Taxa pessimista (margem de segurança)
    if media_proj > 0:
        taxa_pess = media_proj + 0.5 * desvio
    else:
        taxa_pess = media_proj

    # Coeficiente de variação → confiança
    cv = (desvio / media) if media > 0 else 1.0
    confianca = max(5, min(100, int((n / 6) * 100 * max(0, 1 - min(cv, 1)))))

    if media_proj <= 0:
        return {**default, "status": "Estável", "classe": "risco-nulo",
                "media_diaria": round(media, 2), "desvio_padrao": round(desvio, 2),
                "confianca": confianca, "tendencia": round(tendencia_b, 3)}

    dias_central  = qtd / media_proj
    dias_pess     = qtd / taxa_pess if taxa_pess > 0 else dias_central

    # Classificação de risco baseada em dias_central (estimativa principal)
    # Dias pessimistas servem apenas como sinal de antecipação extra quando
    # há variância alta — mas o limiar principal é sobre dias_central.
    #
    #  ≤ 7 dias  → CRÍTICO  (menos de 1 semana)
    #  ≤ 14 dias → ALERTA   (menos de 2 semanas)
    #  > 14 dias → OK
    #
    # Se o cenário pessimista for crítico mas o central ainda for alerta,
    # mantemos em ALERTA para não gerar falsos críticos com poucos dados.
    if dias_central <= 7:
        status, classe = "CRÍTICO", "risco-critico"
    elif dias_central <= 14:
        status, classe = f"⚠ {int(dias_central)}d restantes", "risco-proximo"
    else:
        status, classe = f"{int(dias_central)} dias", "risco-ok"

    return {
        "status":       status,
        "classe":       classe,
        "dias_restantes": round(dias_central, 1),
        "media_diaria": round(media, 2),
        "desvio_padrao": round(desvio, 2),
        "confianca":    confianca,
        "tendencia":    round(tendencia_b, 3),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  ROTAS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # ── Filtros de pesquisa
    busca            = request.args.get('busca', '').strip()
    filtro_campo     = request.args.get('campo', 'nome')
    filtro_categoria = request.args.get('categoria', '')
    filtro_tipo      = request.args.get('tipo', '')
    filtro_risco     = request.args.get('risco', '')

    query  = 'SELECT * FROM itens WHERE 1=1'
    params = []

    if busca:
        campo_map = {'nome': 'nome', 'categoria': 'categoria',
                     'tipo': 'tipo',  'unidade':   'unidade'}
        col = campo_map.get(filtro_campo, 'nome')
        query += f" AND {col} ILIKE %s"
        params.append(f'%{busca}%')

    if filtro_categoria:
        query += " AND categoria = %s"
        params.append(filtro_categoria)

    if filtro_tipo:
        query += " AND tipo = %s"
        params.append(filtro_tipo)

    query += ' ORDER BY nome ASC'
    cur.execute(query, params)
    itens_db = cur.fetchall()

    # Categorias e tipos únicos para os selects
    cur.execute("SELECT DISTINCT categoria FROM itens WHERE categoria IS NOT NULL AND categoria <> '' ORDER BY categoria")
    categorias = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT DISTINCT tipo FROM itens WHERE tipo IS NOT NULL AND tipo <> '' ORDER BY tipo")
    tipos = [r[0] for r in cur.fetchall()]

    produtos = []
    for row in itens_db:
        item = dict(row)
        
        # Garantir que o ID e a Qtd existam para não quebrar a lógica
        item_id = item.get('id')
        # Tenta pegar 'qtd_atual', se não existir tenta 'qtd', se não, usa 0
        qtd_estoque = item.get('qtd_atual', item.get('qtd', 0))
        
        cur.execute('''
            SELECT data, quantidade FROM gastos_diarios
            WHERE item_id = %s ORDER BY data ASC LIMIT 30
        ''', (item_id,))
        gastos = cur.fetchall()

        # Agora passamos a variável segura qtd_estoque
        prev = calcular_previsao(gastos, qtd_estoque)
        item.update(prev)
        
        # Garante que o item tenha a chave qtd_atual para o HTML não quebrar
        item['qtd_atual'] = qtd_estoque 
        
        item['gastos'] = [{'data': str(g[0]), 'quantidade': float(g[1])} for g in gastos]
        item['total_gastos_semana'] = sum(float(g[1]) for g in gastos)
        produtos.append(item)

    cur.close()
    conn.close()

    criticos = sum(1 for p in produtos if p['classe'] == 'risco-critico')
    alertas  = sum(1 for p in produtos if p['classe'] == 'risco-proximo')

    # Filtro por nível de risco (aplicado após cálculo da previsão)
    if filtro_risco:
        produtos = [p for p in produtos if p['classe'] == filtro_risco]

    return render_template('index.html',
        produtos=produtos, categorias=categorias, tipos=tipos,
        busca=busca, filtro_campo=filtro_campo,
        filtro_categoria=filtro_categoria, filtro_tipo=filtro_tipo,
        filtro_risco=filtro_risco,
        total_itens=len(produtos), criticos=criticos, alertas=alertas,
        hoje=str(date.today()))


# ── Registrar gasto diário ────────────────────────────────────────────────────
@app.route('/registrar_gasto', methods=['POST'])
def registrar_gasto():
    item_id    = request.form.get('item_id')
    data_gasto = request.form.get('data_gasto') or str(date.today())
    quantidade = float(request.form.get('quantidade') or 0)

    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute('''
        INSERT INTO gastos_diarios (item_id, data, quantidade)
        VALUES (%s, %s, %s)
        ON CONFLICT (item_id, data) DO UPDATE SET quantidade = EXCLUDED.quantidade
    ''', (item_id, data_gasto, quantidade))
    conn.commit()
    cur.close()
    conn.close()
    flash("Gasto registrado com sucesso!")
    return redirect(url_for('index'))


# ── Fechar semana (item individual) ──────────────────────────────────────────
@app.route('/fechar_semana/<int:item_id>', methods=['POST'])
def fechar_semana(item_id):
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute('SELECT COALESCE(SUM(quantidade),0) FROM gastos_diarios WHERE item_id = %s', (item_id,))
    total = float(cur.fetchone()[0])
    cur.execute('UPDATE itens SET qtd_atual = GREATEST(0, qtd_atual - %s) WHERE id = %s', (total, item_id))
    cur.execute('DELETE FROM gastos_diarios WHERE item_id = %s', (item_id,))
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Semana fechada! {total:g} unidades descontadas do estoque.")
    return redirect(url_for('index'))


# ── Fechar semana (todos) ─────────────────────────────────────────────────────
@app.route('/fechar_semana_todos', methods=['POST'])
def fechar_semana_todos():
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute('SELECT item_id, COALESCE(SUM(quantidade),0) FROM gastos_diarios GROUP BY item_id')
    for item_id, total in cur.fetchall():
        cur.execute('UPDATE itens SET qtd_atual = GREATEST(0, qtd_atual - %s) WHERE id = %s', (total, item_id))
    cur.execute('DELETE FROM gastos_diarios')
    conn.commit()
    cur.close()
    conn.close()
    flash("Semana fechada para todos os itens! Estoques atualizados e gastos resetados.")
    return redirect(url_for('index'))


# ── Adicionar item ────────────────────────────────────────────────────────────
@app.route('/adicionar', methods=['POST'])
def adicionar_item():
    conn = get_db_connection()
    cur  = conn.cursor()
    try:
        cur.execute('''
            INSERT INTO itens (nome, categoria, tipo, unidade, qtd_atual)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (nome) DO UPDATE SET
                categoria = EXCLUDED.categoria,
                tipo      = EXCLUDED.tipo,
                unidade   = EXCLUDED.unidade,
                qtd_atual = EXCLUDED.qtd_atual
        ''', (
            request.form['nome'],
            request.form.get('categoria', ''),
            request.form.get('tipo', ''),
            request.form.get('unidade', ''),
            float(request.form.get('qtd', 0) or 0)
        ))
        conn.commit()
        flash("Item adicionado com sucesso!")
    except Exception as e:
        flash(f"Erro ao adicionar item: {e}")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for('index'))


# ── Upload Excel (Versão Corrigida e Limpa) ──────────────────────────────────
@app.route('/upload_excel', methods=['POST'])
def upload_excel():
    file = request.files.get('file')
    if not file:
        flash("Nenhum arquivo enviado.")
        return redirect(url_for('index'))

    # Data do gasto vem do formulário; padrão = hoje
    data_gasto_str = request.form.get('data_gasto') or str(date.today())
    try:
        data_gasto = datetime.strptime(data_gasto_str, '%Y-%m-%d').date()
    except ValueError:
        data_gasto = date.today()

    try:
        df = pd.read_excel(file)
        df.columns = [str(c).strip() for c in df.columns]

        conn = get_db_connection()
        cur  = conn.cursor()
        itens_sync = 0
        gastos_reg = 0

        for _, row in df.iterrows():
            nome = str(row.get('Item', row.get('item', ''))).strip()
            if not nome or nome.lower() == 'nan':
                continue

            def safe_str(col):
                val = row.get(col, '')
                return '' if str(val).lower() == 'nan' else str(val).strip()

            def safe_num(col):
                try:
                    val = row.get(col, 0)
                    return float(val) if str(val).lower() not in ('nan', '') else 0.0
                except:
                    return 0.0

            categoria   = safe_str('Categoria')
            tipo        = safe_str('Tipo')
            unidade     = safe_str('Unid.')
            qtd_inicial = safe_num('Qtd. Inicial')
            qtd_gasta   = safe_num('Qtd. Gasta')   # ← nova coluna

            # Upsert do cadastro principal
            cur.execute('''
                INSERT INTO itens (nome, categoria, tipo, unidade, qtd_atual)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (nome) DO UPDATE SET
                    categoria = EXCLUDED.categoria,
                    tipo      = EXCLUDED.tipo,
                    unidade   = EXCLUDED.unidade,
                    qtd_atual = EXCLUDED.qtd_atual
            ''', (nome, categoria, tipo, unidade, qtd_inicial))
            itens_sync += 1

            # Se houver gasto preenchido, grava em gastos_diarios com a data escolhida
            if qtd_gasta > 0:
                cur.execute('SELECT id FROM itens WHERE nome = %s', (nome,))
                row_item = cur.fetchone()
                if row_item:
                    cur.execute('''
                        INSERT INTO gastos_diarios (item_id, data, quantidade)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (item_id, data)
                        DO UPDATE SET quantidade = EXCLUDED.quantidade
                    ''', (row_item[0], data_gasto, qtd_gasta))
                    gastos_reg += 1

        conn.commit()
        cur.close()
        conn.close()

        msg = f"Planilha importada! {itens_sync} itens sincronizados"
        if gastos_reg:
            msg += f" · {gastos_reg} gasto(s) registrado(s) para {data_gasto.strftime('%d/%m/%Y')}"
        flash(msg + ".")

    except Exception as e:
        flash(f"Erro ao importar planilha: {e}")

    return redirect(url_for('index'))


# ── Editar item (inline, via fetch) ──────────────────────────────────────────
@app.route('/editar/<int:item_id>', methods=['POST'])
def editar_item(item_id):
    data = request.get_json()
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute('''
        UPDATE itens SET nome=%s, categoria=%s, tipo=%s, unidade=%s, qtd_atual=%s
        WHERE id=%s
    ''', (data['nome'], data['categoria'], data['tipo'],
          data['unidade'], float(data['qtd_atual'] or 0), item_id))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({'ok': True})


# ── Deletar item ──────────────────────────────────────────────────────────────
@app.route('/deletar/<int:item_id>')
def deletar_item(item_id):
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM itens WHERE id = %s", (item_id,))
    conn.commit()
    cur.close()
    conn.close()
    flash("Item removido.")
    return redirect(url_for('index'))


# ── API: detalhes de um item (gastos + previsão) ──────────────────────────────
@app.route('/api/item/<int:item_id>')
def api_item(item_id):
    conn = get_db_connection()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT * FROM itens WHERE id = %s', (item_id,))
    row  = cur.fetchone()
    if not row:
        return jsonify({'error': 'not found'}), 404
    item = dict(row)
    cur.execute('SELECT data, quantidade FROM gastos_diarios WHERE item_id = %s ORDER BY data ASC', (item_id,))
    gastos = cur.fetchall()
    cur.close()
    conn.close()

    prev = calcular_previsao(gastos, item['qtd_atual'])
    return jsonify({
        'item':   {k: str(v) if v is not None else '' for k, v in item.items()},
        'gastos': [{'data': str(g['data']), 'quantidade': float(g['quantidade'])} for g in gastos],
        'previsao': prev
    })


if __name__ == '__main__':
    app.run(debug=True)