#_______________________________________________________________________________#

# PACOTES ####

#_______________________________________________________________________________#

packages <- c('tidyverse','lubridate','stringr','purrr','vroom','data.table','ggplot2','readxl','openxlsx','zoo','rmdformats','formatR')

install_and_load_packages <- function(pckg){
  
  if (!pckg %in% installed.packages()[,1]){ # verifica se um determinado pacote NÃO está instalado
    install.packages(pckg, quiet = T)
  }
  
  library(pckg,character.only = T,quietly = T) # carrega o pacote
  
  return(paste('Package',pckg,'installed and loaded'))
  
}

# aplicando a função para cada entrada (pacote) do vetor

sapply(X = packages,FUN = install_and_load_packages,simplify = T,USE.NAMES = F) 

#_______________________________________________________________________________#

# DIRETORIO ####

#_______________________________________________________________________________#

diretorio_principal <- paste0('C:/Users/marco/OneDrive/Documents/Portfolio/dados_epe')

#_______________________________________________________________________________#

# DOWNLOAD DOS DADOS ####

#_______________________________________________________________________________#

.url <- "https://www.epe.gov.br/sites-pt/publicacoes-dados-abertos/publicacoes/Documents/CONSUMO%20MENSAL%20DE%20ENERGIA%20EL%c3%89TRICA%20POR%20CLASSE.xls"

.destfile <- paste(
  diretorio_principal,
  'CONSUMO MENSAL DE ENERGIA ELÉTRICA POR CLASSE.xls',
  sep='/'
)

download.file(url = .url,destfile = .destfile,quiet = T,mode = 'wb')

#_______________________________________________________________________________#

# LEITURA DOS DADOS ####

#_______________________________________________________________________________#

.arq_epe <- .destfile

.abas <- excel_sheets(.arq_epe)

leitura_dados_epe <- function(aba,arq = .arq_epe){
  
  # cat('>> Aba',aba,'\n') esta linha serve para printar qual aba está sendo lida. No arquivo .RMD não faz sentido mantê-la, por isso comentei. Para um código de automação, será útil!
  
  tabela <- read_xls(path = .arq_epe,sheet = aba) %>% 
    suppressMessages %>% 
    suppressWarnings
  
  return(tabela)
  
}

dados_epe <- map(.x = .abas,.f = leitura_dados_epe) %>% 
  setNames(.abas)

#_______________________________________________________________________________#

# TRATAMENTO DOS DADOS POR ABA ####

#_______________________________________________________________________________#

tratamento_dados_epe <- function(aba, lista = dados_epe){
  
  # cat('>> Aba',aba,'\n') esta linha serve para printar qual aba está sendo lida. No arquivo .RMD não faz sentido mantê-la, por isso comentei. Para um código de automação, será útil!
  
  tabela <- lista[[aba]] %>% # indexando a tabela correspondente à respectiva aba na lista
    # criando a coluna temporária "atributo_1" para posterior remoção da coluna "aba"
    mutate(atributo_1 = aba) %>% 
    # criando a coluna temporária "atributo_2": consiste na primeira linha [1] da primeira coluna [[1]] da tabela
    mutate(atributo_2 = .[[1]][1]) %>% 
    slice(-1:-3) # fatiando a tabela: removendo as linhas de 1 a 3
  
  # caso o nome da aba referida esteja entre a primeira 
  # e a aba cujo nome é CONSUMIDORES TOTAIS, o tratamento dos dados será um; 
  # caso contrário, haverá outro, após o "else"
  if (aba %in% .abas[1:which(.abas == 'CONSUMIDORES TOTAIS')]){ 
    
    tabela %<>% 
      # criando a coluna "ano" utilizando expressões regulares na segunda coluna
      mutate(ano = str_extract(.[[2]],'^[0-9]{4}(|\\*)$') %>% str_extract('[0-9]+')) %>% 
      # populando a coluna "ano" a partir do único valor não-nulo detectado pelas expressões regulares na linha acima
      mutate(ano = na.locf0(ano)) %>% 
      # utilizando expressões regulares para criar a coluna temporária "atributo_3" a partir da primeira coluna
      mutate(atributo_3 = str_extract(.[[1]],'REGIÃO GEOGRÁFICA|SUBSISTEMA ELÉTRICO|SUBSISTEMA')) %>% 
      # realizando modificações condicionais na coluna criada
      mutate(atributo_3 = ifelse(grepl('SUBSISTEMA',atributo_3),'SUBSISTEMA ELÉTRICO',atributo_3)) %>% 
      # renomeando condicionalmente as colunas da tabela tratada até o momento
      rename_with( 
        # utilizando expressões regulares para selecionar as 
        # colunas que devem ser renomeadas (todas que forem do tipo "...2")
        .cols = matches('\\.\\.\\.[0-9]+$'),
        # aplicando uma função para renomear
        .fn = function(x){ 
          
          # verificando a quantidade de colunas a serem renomeadas
          tamanho_colunas_meses_anos <- select(.,matches('\\.\\.\\.[0-9]+$')) %>% ncol 
          
          if (tamanho_colunas_meses_anos == 13){
            # caso a quantidade de colunas seja 13, os novos nomes serão estes
            colunas_meses_anos <- c(1:12,'Total_Ano') 
          } else {
            # caso contrário, estes!
            colunas_meses_anos <- as.character(1:12) 
          }
          
          x <- colunas_meses_anos # nomes finais
          
          return(x) # resultado retornado
          
        }
      ) %>% 
      # renomeando a primeira coluna para "atributo_4"
      rename(atributo_4 = 1) %>% 
      # utilizando expressões regulares para selecionar apenas as colunas que não contenham "Total_Ano" em seu nome
      select(-matches('Total_Ano')) %>% 
      # utilizando expressões regulares para filtrar dados na coluna "atributo_4":
      # não queremos linhas contendo os termos TOTAL e NC nesta coluna
      filter(!grepl('^TOTAL|^NC ',atributo_4)) %>% 
      # novos filtros, mais específicos: não queremos "TOTAL BRASIL" e nem valores nulos (NA)
      filter(atributo_4 != 'TOTAL BRASIL',!is.na(atributo_4)) %>% 
      # utilizando novamente o na.locf0 para substituir valores nulos pelos não-nulos mais recentes
      mutate(atributo_3 = na.locf0(atributo_3)) %>%
      # removendo valores nulos da coluna denominada `1` (não confundir com a coluna de índice 1!)
      filter(!is.na(`1`)) %>% 
      # convertendo para tipo numérico as colunas nomeadas apenas com números (expressões regulares aqui novamente)
      mutate(across(matches('^[0-9]+$'),as.numeric)) %>% 
      # transformando em linhas as colunas nomeadas apenas com números: 
      # as colunas de 1 a 12 serão transformadas na coluna chamada "mes", 
      # e seus valores atrelados serão armazenados na coluna "valor"
      pivot_longer(cols = matches('^[0-9]+$'),names_to = 'mes',values_to = 'valor') %>%
      # criando a coluna de data ("ano-mes-dia", onde dia = 1) a partir das colunas "ano" e "mes"
      mutate(data = as.Date(paste(ano,mes,1,sep='-'))) %>% 
      # selecionando somente as colunas do tipo "atributo_X", além de "data" e "valor"
      select(all_of(c(paste0('atributo_',1:4),'data','valor'))) %>% 
      # aplicando transformações de string
      mutate(atributo_4 = ifelse(atributo_4 ==  'C.OESTE','Centro-Oeste',str_to_title(atributo_4))) %>% 
      # novas modificações na coluna "atributo_3": detectando as linhas que são classes de consumo
      mutate(atributo_3 = ifelse(grepl('Resid|Comer|Indus|Outros',atributo_4),'CLASSE',atributo_3))
    
  } else {
    
    # renomeando dinamicamente as variáveis de forma que a primeira coluna seja "atributo_3", 
    # e as subsequentes o respectivo ano e mês associados da tabela
    names(tabela)[-((length(names(tabela))-1):length(names(tabela)))] <- tabela %>% 
      slice(1) %>% # extraindo a primeira linha da tabela, pois contém os anos
      t %>% as.vector %>% # transpondo e transformando em vetor (necessário para utilizar na nomeação)
      # extraindo, com expressões regulares, somente números com 4 algarismos seguido ou não de um * ao final
      str_extract(.,'^[0-9]{4}(\\*|)$') %>% 
      str_remove_all(.,'[^0-9]') %>% # removendo tudo o que não é numérico
      .[-which(is.na(.))] %>% # removendo tudo o que é NA
      rep(.,each = 12) %>% # repetindo cada ano 12 vezes para criar o par "ano_mes"
      paste0(.,'_',1:12) %>% # inserindo os meses em cada um dos anos
      c('atributo_3',.) # concatenando "atributo_3" no inicio do vetor para finalizar
    
    tabela %<>%
      # filtrando somente pelas linhas não-nulas na coluna 3 para preservar somente os dados relevantes
      filter(!is.na(.[[3]])) %>% 
      # filtrando somente pelas linhas não-nulas na coluna "atributo_3" para preservar somente os dados relevantes
      filter(!is.na(atributo_3)) %>%
      # removendo, com expressões regulares, as linhas que começam com TOTAL na coluna "atributo_3"
      filter(!grepl('^TOTAL',atributo_3)) %>% 
      # transformando em numéricas, utilizando expressões regulares, 
      # as colunas que possuem o padrão "YYYY_M" ou "YYYY_MM"
      mutate(across(matches('^[0-9]{4}_[0-9]+$'),as.numeric)) %>% 
      # transformando em linhas as colunas nomeadas apenas com números: as colunas serão transformadas 
      # na coluna chamada "ano_mes", e seus valores atrelados serão armazenados na coluna "valor"
      pivot_longer(cols = matches('^[0-9]{4}_[0-9]+$'),names_to = 'ano_mes',values_to = 'valor') %>%
      # separando a coluna "ano_mes" em duas: "ano" e "mes"
      separate(col = ano_mes,into = c('ano','mes'),sep = '_') %>% 
      # criando a coluna de data a partir das colunas ano e mes
      mutate(data = as.Date(paste(ano,mes,1,sep='-'))) %>% 
      # criando a coluna atributo_4 com valores nulos para que se possa juntar 
      # às outras tabelas que possuem tal coluna preenchida
      mutate(atributo_4 = as.character(NA)) %>% 
      # selecionando somente as colunas do tipo "atributo_i", "data" e "valor"
      select(all_of(c(paste0('atributo_',1:4),'data','valor'))) 
    
  }
  
  return(tabela)
  
}

dados_epe_abas_tratadas <- map_dfr(.abas,tratamento_dados_epe)

#_______________________________________________________________________________#

# TRATAMENTO FINAL DOS DADOS ####

#_______________________________________________________________________________#

tratamento_dados_completos_epe <- function(tabela){
  
  tabela_mercado_total_classe_regiao_subsistema <- tabela %>% 
    filter(!atributo_1 %in% c('TOTAL','CONSUMO POR UF'), !grepl('CATIVO|INDUSTRIAL GENERO|POR (U|)F$',atributo_1)) %>% 
    mutate(classe = str_extract(atributo_1,'(RESIDENCIA|INDUSTRIA|COMERCIA|TOTA)(IS|L)$|OUTROS')) %>% 
    mutate(classe = ifelse(grepl('TOTA',classe),'NÃO RESIDENCIAL',str_replace(classe,'IS$','L'))) %>% 
    mutate(dado = str_extract(atributo_2,'Consumo|consumidores') %>% str_to_upper) %>% 
    mutate(abertura = atributo_3) %>% 
    mutate(atributo = atributo_4 %>% str_to_upper) %>% 
    mutate(mercado = 'TOTAL') %>% 
    select(-matches('atributo_')) %>% 
    select(mercado,dado,classe,abertura,atributo,data,valor)
  
  tabela_mercado_total_classe_ramo <- tabela %>% 
    filter(atributo_1 %in% 'INDUSTRIAL GENERO') %>% 
    mutate(classe = 'INDUSTRIAL') %>% 
    mutate(dado = 'CONSUMO') %>% 
    mutate(abertura = 'RAMO') %>% 
    mutate(atributo = atributo_3) %>% 
    mutate(mercado = 'TOTAL') %>% 
    select(-matches('atributo_')) %>% 
    select(mercado,dado,classe,abertura,atributo,data,valor)
  
  tabela_mercado_total_classe_uf <- tabela %>% 
    filter(grepl('POR (U|)F$',atributo_1),!grepl('CATIVO|CONSUMO POR UF',atributo_1)) %>% 
    mutate(classe = str_extract(atributo_1,'(RESIDENCIA|INDUSTRIA|COMERCIA|TOTA)(IS|L)|OUTROS')) %>% 
    mutate(classe = ifelse(grepl('TOTA',classe),'NÃO RESIDENCIAL',str_replace(classe,'IS$','L'))) %>% 
    mutate(dado = str_extract(atributo_2,'Consumo|consumidores') %>% str_to_upper) %>% 
    mutate(abertura = 'UF') %>% 
    mutate(atributo = atributo_3 %>% str_to_upper) %>% 
    mutate(mercado = 'TOTAL') %>% 
    select(-matches('atributo_')) %>% 
    select(mercado,dado,classe,abertura,atributo,data,valor)
  
  tabela_mercado_cativo_classe_regiao_subsistema <- tabela %>% 
    filter(atributo_1 %in% 'CATIVO') %>% 
    mutate(classe = str_to_upper(atributo_4) %>% str_extract(.,'(RESIDENCIA|INDUSTRIA|COMERCIA|TOTA)(IS|L)|OUTROS')) %>% 
    mutate(classe = ifelse(grepl('TOTA',classe),'NÃO RESIDENCIAL',str_replace(classe,'IS$','L'))) %>% 
    mutate(classe = ifelse(is.na(classe),'TOTAL',classe)) %>% 
    mutate(dado = str_extract(atributo_2,'Consumo|consumidores') %>% str_to_upper) %>% 
    mutate(abertura = atributo_3) %>% 
    mutate(atributo = atributo_4 %>% str_to_upper) %>% 
    mutate(mercado = 'CATIVO') %>% 
    select(-matches('atributo_')) %>% 
    mutate(across(c(abertura,atributo), ~ifelse(classe != 'TOTAL','TOTAL',.))) %>% 
    select(mercado,dado,classe,abertura,atributo,data,valor)
  
  tabela_mercado_cativo_classe_uf <- tabela %>% 
    filter(atributo_1 %in% 'CONSUMO CATIVO POR UF') %>% 
    mutate(classe = 'TOTAL') %>% 
    mutate(dado = str_extract(atributo_2,'Consumo|consumidores') %>% str_to_upper) %>% 
    mutate(abertura = 'UF') %>% 
    mutate(atributo = atributo_3 %>% str_to_upper) %>% 
    mutate(mercado = 'CATIVO') %>% 
    select(-matches('atributo_')) %>% 
    select(mercado,dado,classe,abertura,atributo,data,valor)
  
  tabela_trat <- bind_rows(
    tabela_mercado_total_classe_regiao_subsistema,
    tabela_mercado_total_classe_ramo,
    tabela_mercado_total_classe_uf,
    tabela_mercado_cativo_classe_regiao_subsistema,
    tabela_mercado_cativo_classe_uf
  ) %>% 
    mutate(chave_seletora = paste(dado,'-','MERCADO',mercado,'-','CLASSE',classe,'-','POR',abertura)) %>% 
    select(chave_seletora,everything())
  
  return(tabela_trat)
}

dados_epe_tratados_final <- tratamento_dados_completos_epe(dados_epe_abas_tratadas)

dados_epe_tratados_final

#_______________________________________________________________________________#

# EXPORTACAO DOS DADOS ####

#_______________________________________________________________________________#

saveRDS(object = dados_epe_tratados_final,file = 'dados_epe_tratados.RDS')