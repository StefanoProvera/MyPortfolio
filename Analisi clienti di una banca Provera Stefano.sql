/*
STEFANO PROVERA

DESCRIZIONE DEL PROGETTO
L'azienda Banking Intelligence vuole sviluppare un modello di machine learning supervisionato per prevedere i comportamenti futuri dei propri clienti,
basandosi sui dati transazionali e sulle caratteristiche del possesso di prodotti. Lo scopo del progetto è creare una tabella denormalizzata con una
serie di indicatori (feature) derivati dalle tabelle disponibili nel database, che rappresentano i comportamenti e le attività finanziarie dei clienti.

OBIETTIVO
Il nostro obiettivo è creare una tabella di feature per il training di modelli di machine learning, arricchendo i dati dei clienti con vari indicatori 
calcolati a partire dalle loro transazioni e dai conti posseduti. La tabella finale sarà riferita all'ID cliente e conterrà informazioni sia di tipo
quantitativo che qualitativo.

VALORE AGGIUNTO
La tabella denormalizzata permetterà di estrarre feature comportamentali avanzate per l'addestramento 
di modelli di machine learning supervisionato, fornendo numerosi vantaggi per l'azienda:

- Predizione del comportamento dei clienti: Analizzando le transazioni e il possesso di prodotti, 
	si possono identificare pattern di comportamento utili per prevedere azioni future come l'acquisto di nuovi prodotti o la chiusura di conti.
- Riduzione del tasso di abbandono: Utilizzando gli indicatori comportamentali, si può costruire un modello per identificare i clienti 
	a rischio di abbandono, permettendo interventi tempestivi da parte del team di marketing.
- Miglioramento della gestione del rischio: La segmentazione basata su comportamenti finanziari 
	consente di individuare clienti ad alto rischio e ottimizzare le strategie di credito e rischio.
- Personalizzazione delle offerte: Le feature estratte possono essere utilizzate per personalizzare 
	offerte di prodotti e servizi in base alle abitudini e preferenze dei singoli clienti, aumentando così la customer satisfaction.
- Prevenzione delle frodi: Attraverso l’analisi delle transazioni per tipologia e importi, 
	il modello può rilevare anomalie comportamentali indicative di frodi, migliorando le strategie di sicurezza e prevenzione.

Questi vantaggi porteranno un miglioramento complessivo delle operazioni aziendali, 
consentendo una maggiore efficienza nella gestione dei clienti e una crescita sostenibile del business.

INDICATORI DA CALCOLARE
Indicatori di base: 
- Età del cliente (da tabella cliente).

Indicatori sulle transazioni
- Numero di transazioni in uscita su tutti i conti.
- Numero di transazioni in entrata su tutti i conti.
- Importo totale transato in uscita su tutti i conti.
- Importo totale transato in entrata su tutti i conti.

Indicatori sui conti
- Numero totale di conti posseduti.
- Numero di conti posseduti per tipologia (un indicatore per ogni tipo di conto).

Indicatori sulle transazioni per tipologia di conto
- Numero di transazioni in uscita per tipologia di conto (un indicatore per tipo di conto).
- Numero di transazioni in entrata per tipologia di conto (un indicatore per tipo di conto).
- Importo transato in uscita per tipologia di conto (un indicatore per tipo di conto).
- Importo transato in entrata per tipologia di conto (un indicatore per tipo di conto).
*/

-- Comincio da un select * per andare ad analizzare i campi e la struttura delle varie tabelle nel database
select * from banca.cliente limit 10;
select * from banca.conto limit 10;
select * from banca.conto order by id_cliente limit 10;
select * from banca.tipo_conto limit 10;
select * from banca.tipo_transazione limit 10;
select * from banca.transazioni limit 10;

-- la tabelle dei fatti è quella con le transazioni effettuate, poi abbiamo le varie altre tabelle dimensionali:
-- cliente, conto, tipo conto e tipo transazione

-- al seguente link si può visualizzare il diagramma entità relazione del database in questione
-- https://drive.google.com/file/d/1lIxJDlqfjfjl_pT1iFYrOUrhFpb6yvBl/view?usp=sharing

-- inizio facendo un controllo delle chiavi di join per verificare non vi siano duplicati 
-- (per le tabelle dimensionali dove le chiavi di join sono chiavi primarie) 
-- per evitare duplicazioni di record mentre effetto le join

select id_tipo_transazione, count(*) as duplicati from banca.tipo_transazione group by id_tipo_transazione having count(*) >1;
select id_conto, count(*) as duplicati from banca.conto group by id_conto having count(*) >1;
select id_cliente, count(*) as duplicati from banca.cliente group by id_cliente having count(*) >1;
select id_tipo_conto, count(*) as duplicati from banca.tipo_conto group by id_tipo_conto having count(*) >1;

-- ok perfetto nessun duplicato

-- verifico anche se le foreign keys hanno per caso valori null 
-- ("valore null" licenza poetica, so che null non è un valore ma assenza di valore)
select * from banca.transazioni where (id_tipo_trans is null or id_conto is null);
select * from banca.conto where (id_cliente is null or id_tipo_conto is null);

-- ok perfetto anche qui non abbiamo null

-- verifico che prima e post join le righe rimangano in egual numero e non ci siano duplicazioni di valori
select count(*) from banca.transazioni;

select count(*) from banca.transazioni tran
left join banca.conto conto
on tran.id_conto = conto.id_conto
left join banca.tipo_transazione tptr
on tran.id_tipo_trans = tptr.id_tipo_transazione

left join banca.cliente cl
on conto.id_cliente = cl.id_cliente
left join banca.tipo_conto tpcn
on conto.id_tipo_conto = tpcn.id_tipo_conto;

-- pre e post join il numero di record è rimasto invariato --> tutto ok 


-- procedo con la creazione della tabella denormalizzata

-- mi faccio un esempio di query per capire come calcolare l'età
select id_cliente,  TIMESTAMPDIFF(YEAR, data_nascita, CURDATE()) as eta from banca.cliente;

-- esempio di query per calcolare indicatori sulle transazioni
select conto.id_cliente, 
sum(case when segno = '+' then 1 else 0 end) as transazioni_in_entrata,
sum(case when segno = '-' then 1 else 0 end) as transazioni_in_uscita,
sum(case when segno = '+' then importo else 0 end) as importo_in_entrata,
sum(case when segno = '-' then importo else 0 end) as importo_in_uscita
from banca.transazioni tran
left join banca.conto conto
on tran.id_conto = conto.id_conto
left join banca.tipo_transazione tptr
on tran.id_tipo_trans = tptr.id_tipo_transazione
group by conto.id_cliente
order by id_cliente;


-- esempio di query per calcolare indicatori sui conti
select 
id_cliente,
count(*) as conti_posseduti,
sum(case when tpcn.id_tipo_conto = 0 then 1 else 0 end) as nr_conti_base,
sum(case when tpcn.id_tipo_conto = 1 then 1 else 0 end) as nr_conti_business,
sum(case when tpcn.id_tipo_conto = 2 then 1 else 0 end) as nr_conti_privati,
sum(case when tpcn.id_tipo_conto = 3 then 1 else 0 end) as nr_conti_famiglie
from banca.conto conto
left join banca.tipo_conto tpcn
on conto.id_tipo_conto = tpcn.id_tipo_conto
group by id_cliente
order by id_cliente;


-- esempio di query per calcolare indicatori sulle transazioni per tipologia di conto
select 
id_cliente,
sum(case when tpcn.id_tipo_conto = 0 and tptr.segno ='+' then 1 else 0 end) as nr_transazioni_in_entrata_conto_base,
sum(case when tpcn.id_tipo_conto = 0 and tptr.segno ='-' then 1 else 0 end) as nr_transazioni_in_uscita_conto_base,
sum(case when tpcn.id_tipo_conto = 1 and tptr.segno ='+' then 1 else 0 end) as nr_transazioni_in_entrata_conto_business,
sum(case when tpcn.id_tipo_conto = 1 and tptr.segno ='-' then 1 else 0 end) as nr_transazioni_in_uscita_conto_business,
sum(case when tpcn.id_tipo_conto = 2 and tptr.segno ='+' then 1 else 0 end) as nr_transazioni_in_entrata_conto_privati,
sum(case when tpcn.id_tipo_conto = 2 and tptr.segno ='-' then 1 else 0 end) as nr_transazioni_in_uscita_conto_privati,
sum(case when tpcn.id_tipo_conto = 3 and tptr.segno ='+' then 1 else 0 end) as nr_transazioni_in_entrata_conto_famiglie,
sum(case when tpcn.id_tipo_conto = 3 and tptr.segno ='-' then 1 else 0 end) as nr_transazioni_in_uscita_conto_famiglie,
sum(case when tpcn.id_tipo_conto = 0 and tptr.segno ='+' then importo else 0 end) as importo_in_entrata_conto_base,
sum(case when tpcn.id_tipo_conto = 0 and tptr.segno ='-' then importo else 0 end) as importo_in_uscita_conto_base,
sum(case when tpcn.id_tipo_conto = 1 and tptr.segno ='+' then importo else 0 end) as importo_in_entrata_conto_business,
sum(case when tpcn.id_tipo_conto = 1 and tptr.segno ='-' then importo else 0 end) as importo_in_uscita_conto_business,
sum(case when tpcn.id_tipo_conto = 2 and tptr.segno ='+' then importo else 0 end) as importo_in_entrata_conto_privati,
sum(case when tpcn.id_tipo_conto = 2 and tptr.segno ='-' then importo else 0 end) as importo_in_uscita_conto_privati,
sum(case when tpcn.id_tipo_conto = 3 and tptr.segno ='+' then importo else 0 end) as importo_in_entrata_conto_famiglie,
sum(case when tpcn.id_tipo_conto = 3 and tptr.segno ='-' then importo else 0 end) as importo_in_uscita_conto_famiglie
 from banca.transazioni tran
left join banca.conto conto
on tran.id_conto = conto.id_conto
left join banca.tipo_transazione tptr
on tran.id_tipo_trans = tptr.id_tipo_transazione
left join banca.tipo_conto tpcn
on conto.id_tipo_conto = tpcn.id_tipo_conto
group by id_cliente;


-- ok adesso ho correttamente creato tutte le query che mi serviranno per formare la tabella definitiva con tutti gli indicatori;
-- trasformo tutte le query precedenti in cte e poi vado a joinarle alla tabella cliente

-- aggiungo un drop table if exists all'inizio, qualora la tabella sia esistente
DROP TABLE IF EXISTS banca.tabella_denormalizzata;

-- uso il comando create table as select per creare la tabella denormalizzata, 
-- così facendo non devo andare a creare manualmente la tabella con il suo schema
create table banca.tabella_denormalizzata as

with eta_cliente as 
(select id_cliente,  TIMESTAMPDIFF(YEAR, data_nascita, CURDATE()) as eta from banca.cliente)
, transazioni_indicatori as
(select conto.id_cliente, 
sum(case when segno = '+' then 1 else 0 end) as transazioni_in_entrata,
sum(case when segno = '-' then 1 else 0 end) as transazioni_in_uscita,
sum(case when segno = '+' then importo else 0 end) as importo_in_entrata,
sum(case when segno = '-' then importo else 0 end) as importo_in_uscita
from banca.transazioni tran
left join banca.conto conto
on tran.id_conto = conto.id_conto
left join banca.tipo_transazione tptr
on tran.id_tipo_trans = tptr.id_tipo_transazione
group by conto.id_cliente)
, conto_indicatori as
(select 
id_cliente,
count(*) as conti_posseduti,
sum(case when tpcn.id_tipo_conto = 0 then 1 else 0 end) as nr_conti_base,
sum(case when tpcn.id_tipo_conto = 1 then 1 else 0 end) as nr_conti_business,
sum(case when tpcn.id_tipo_conto = 2 then 1 else 0 end) as nr_conti_privati,
sum(case when tpcn.id_tipo_conto = 3 then 1 else 0 end) as nr_conti_famiglie
from banca.conto conto
left join banca.tipo_conto tpcn
on conto.id_tipo_conto = tpcn.id_tipo_conto
group by id_cliente)
, transazioni_per_conto_indicatori as
(select 
id_cliente,
sum(case when tpcn.id_tipo_conto = 0 and tptr.segno ='+' then 1 else 0 end) as nr_transazioni_in_entrata_conto_base,
sum(case when tpcn.id_tipo_conto = 0 and tptr.segno ='-' then 1 else 0 end) as nr_transazioni_in_uscita_conto_base,
sum(case when tpcn.id_tipo_conto = 1 and tptr.segno ='+' then 1 else 0 end) as nr_transazioni_in_entrata_conto_business,
sum(case when tpcn.id_tipo_conto = 1 and tptr.segno ='-' then 1 else 0 end) as nr_transazioni_in_uscita_conto_business,
sum(case when tpcn.id_tipo_conto = 2 and tptr.segno ='+' then 1 else 0 end) as nr_transazioni_in_entrata_conto_privati,
sum(case when tpcn.id_tipo_conto = 2 and tptr.segno ='-' then 1 else 0 end) as nr_transazioni_in_uscita_conto_privati,
sum(case when tpcn.id_tipo_conto = 3 and tptr.segno ='+' then 1 else 0 end) as nr_transazioni_in_entrata_conto_famiglie,
sum(case when tpcn.id_tipo_conto = 3 and tptr.segno ='-' then 1 else 0 end) as nr_transazioni_in_uscita_conto_famiglie,
sum(case when tpcn.id_tipo_conto = 0 and tptr.segno ='+' then importo else 0 end) as importo_in_entrata_conto_base,
sum(case when tpcn.id_tipo_conto = 0 and tptr.segno ='-' then importo else 0 end) as importo_in_uscita_conto_base,
sum(case when tpcn.id_tipo_conto = 1 and tptr.segno ='+' then importo else 0 end) as importo_in_entrata_conto_business,
sum(case when tpcn.id_tipo_conto = 1 and tptr.segno ='-' then importo else 0 end) as importo_in_uscita_conto_business,
sum(case when tpcn.id_tipo_conto = 2 and tptr.segno ='+' then importo else 0 end) as importo_in_entrata_conto_privati,
sum(case when tpcn.id_tipo_conto = 2 and tptr.segno ='-' then importo else 0 end) as importo_in_uscita_conto_privati,
sum(case when tpcn.id_tipo_conto = 3 and tptr.segno ='+' then importo else 0 end) as importo_in_entrata_conto_famiglie,
sum(case when tpcn.id_tipo_conto = 3 and tptr.segno ='-' then importo else 0 end) as importo_in_uscita_conto_famiglie
 from banca.transazioni tran
left join banca.conto conto
on tran.id_conto = conto.id_conto
left join banca.tipo_transazione tptr
on tran.id_tipo_trans = tptr.id_tipo_transazione
left join banca.tipo_conto tpcn
on conto.id_tipo_conto = tpcn.id_tipo_conto
group by id_cliente)

select a.*,
eta,
case when conti_posseduti is null then 0 else conti_posseduti end as conti_posseduti,
/*
nr_conti_base,
nr_conti_business,
nr_conti_privati,
nr_conti_famiglie,
transazioni_in_entrata,
transazioni_in_uscita,
nr_transazioni_in_entrata_conto_base,
nr_transazioni_in_uscita_conto_base,
nr_transazioni_in_entrata_conto_business,
nr_transazioni_in_uscita_conto_business,
nr_transazioni_in_entrata_conto_privati,
nr_transazioni_in_uscita_conto_privati,
nr_transazioni_in_entrata_conto_famiglie,
nr_transazioni_in_uscita_conto_famiglie,
importo_in_entrata,
importo_in_uscita,
importo_in_entrata_conto_base,
importo_in_uscita_conto_base,
importo_in_entrata_conto_business,
importo_in_uscita_conto_business,
importo_in_entrata_conto_privati,
importo_in_uscita_conto_privati,
importo_in_entrata_conto_famiglie,
importo_in_uscita_conto_famiglie
*/
case when nr_conti_base is null then 0 else nr_conti_base end as nr_conti_base,
case when nr_conti_business is null then 0 else nr_conti_business end as nr_conti_business,
case when nr_conti_privati is null then 0 else nr_conti_privati end as nr_conti_privati,
case when nr_conti_famiglie is null then 0 else nr_conti_famiglie end as nr_conti_famiglie,
case when transazioni_in_entrata is null then 0 else transazioni_in_entrata end as transazioni_in_entrata,
case when transazioni_in_uscita is null then 0 else transazioni_in_uscita end as transazioni_in_uscita,
case when nr_transazioni_in_entrata_conto_base is null then 0 else nr_transazioni_in_entrata_conto_base end as nr_transazioni_in_entrata_conto_base,
case when nr_transazioni_in_uscita_conto_base is null then 0 else nr_transazioni_in_uscita_conto_base end as nr_transazioni_in_uscita_conto_base,
case when nr_transazioni_in_entrata_conto_business is null then 0 else nr_transazioni_in_entrata_conto_business end as nr_transazioni_in_entrata_conto_business,
case when nr_transazioni_in_uscita_conto_business is null then 0 else nr_transazioni_in_uscita_conto_business end as nr_transazioni_in_uscita_conto_business,
case when nr_transazioni_in_entrata_conto_privati is null then 0 else nr_transazioni_in_entrata_conto_privati end as nr_transazioni_in_entrata_conto_privati,
case when nr_transazioni_in_uscita_conto_privati is null then 0 else nr_transazioni_in_uscita_conto_privati end as nr_transazioni_in_uscita_conto_privati,
case when nr_transazioni_in_entrata_conto_famiglie is null then 0 else nr_transazioni_in_entrata_conto_famiglie end as nr_transazioni_in_entrata_conto_famiglie,
case when nr_transazioni_in_uscita_conto_famiglie is null then 0 else nr_transazioni_in_uscita_conto_famiglie end as nr_transazioni_in_uscita_conto_famiglie,
case when importo_in_entrata is null then 0 else importo_in_entrata end as importo_in_entrata,
case when importo_in_uscita is null then 0 else importo_in_uscita end as importo_in_uscita,
case when importo_in_entrata_conto_base is null then 0 else importo_in_entrata_conto_base end as importo_in_entrata_conto_base,
case when importo_in_uscita_conto_base is null then 0 else importo_in_uscita_conto_base end as importo_in_uscita_conto_base,
case when importo_in_entrata_conto_business is null then 0 else importo_in_entrata_conto_business end as importo_in_entrata_conto_business,
case when importo_in_uscita_conto_business is null then 0 else importo_in_uscita_conto_business end as importo_in_uscita_conto_business,
case when importo_in_entrata_conto_privati is null then 0 else importo_in_entrata_conto_privati end as importo_in_entrata_conto_privati,
case when importo_in_uscita_conto_privati is null then 0 else importo_in_uscita_conto_privati end as importo_in_uscita_conto_privati,
case when importo_in_entrata_conto_famiglie is null then 0 else importo_in_entrata_conto_famiglie end as importo_in_entrata_conto_famiglie,
case when importo_in_uscita_conto_famiglie is null then 0 else importo_in_uscita_conto_famiglie end as importo_in_uscita_conto_famiglie
from banca.cliente a
left join eta_cliente b
on a.id_cliente=b.id_cliente
left join transazioni_indicatori c
on a.id_cliente=c.id_cliente
left join conto_indicatori d
on a.id_cliente=d.id_cliente
left join transazioni_per_conto_indicatori e
on a.id_cliente=e.id_cliente;

-- verifico che sia tutto ok nella tabella
select * from  banca.tabella_denormalizzata


