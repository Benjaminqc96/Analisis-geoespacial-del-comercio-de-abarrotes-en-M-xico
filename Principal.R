
library(dplyr)
library(sparklyr)
library(ggplot2)
library(sf)
library(readxl)
library(ggforce)
library(ggspatial)

mapa <- read_sf('Estados.json')
mapa_mun <- read_sf('Municipios.json')

base_cp <- read.csv(file = 'cod_pos.csv',
                    stringsAsFactors = F)

base_coor <- read_xlsx(path = 'base_oxxo.xlsx') %>% 
  na.omit() %>% distinct()


base_coor$Estado <- NA
base_coor$Mun <- NA

est_mun <- base_cp %>% select(c_estado, c_mnpio) %>% distinct()

for (i in 1:nrow(est_mun)) {
  cp <- base_cp %>% filter(c_estado == est_mun$c_estado[i], 
                           c_mnpio == est_mun$c_mnpio[i]) %>% select(d_codigo) %>% unlist()
  
  base_coor$Estado[base_coor$CODIGO %in% cp] <- est_mun$c_estado[i]
  base_coor$Mun[base_coor$CODIGO %in% cp] <- est_mun$c_mnpio[i]
  
}


guia_cp <- base_cp %>% select(c_estado, c_mnpio) %>% distinct() %>% 
  arrange(c_estado, c_mnpio)

guia_map <- tibble(edo = as.numeric(mapa_mun$cvegeoedo), 
                   mun = as.numeric(mapa_mun$cve_mun)) %>% 
  arrange(edo, mun)


guia_cp$man <- paste(guia_cp$c_estado, guia_cp$c_mnpio)
guia_map$man <- paste(guia_map$edo, guia_map$mun)




## correciones 

#guia_cp$man[guia_cp$man %in% guia_map$man == F]

base_cp$c_mnpio[base_cp$c_estado == 2 & base_cp$c_mnpio == 6] <- 5
base_cp$c_mnpio[base_cp$c_estado == 2 & base_cp$c_mnpio == 7] <- 5

base_cp$c_mnpio[base_cp$c_estado == 4 & base_cp$c_mnpio == 12] <- 11
base_cp$c_mnpio[base_cp$c_estado == 4 & base_cp$c_mnpio == 13] <- 11

base_cp$c_mnpio[base_cp$c_estado == 7 & base_cp$c_mnpio == 120] <- 119
base_cp$c_mnpio[base_cp$c_estado == 7 & base_cp$c_mnpio == 121] <- 119
base_cp$c_mnpio[base_cp$c_estado == 7 & base_cp$c_mnpio == 122] <- 119
base_cp$c_mnpio[base_cp$c_estado == 7 & base_cp$c_mnpio == 123] <- 119
base_cp$c_mnpio[base_cp$c_estado == 7 & base_cp$c_mnpio == 124] <- 119
base_cp$c_mnpio[base_cp$c_estado == 7 & base_cp$c_mnpio == 125] <- 119

base_cp$c_mnpio[base_cp$c_estado == 12 & base_cp$c_mnpio == 82] <- 81
base_cp$c_mnpio[base_cp$c_estado == 12 & base_cp$c_mnpio == 83] <- 81
base_cp$c_mnpio[base_cp$c_estado == 12 & base_cp$c_mnpio == 84] <- 81
base_cp$c_mnpio[base_cp$c_estado == 12 & base_cp$c_mnpio == 85] <- 81

base_cp$c_mnpio[base_cp$c_estado == 17 & base_cp$c_mnpio == 34] <- 33
base_cp$c_mnpio[base_cp$c_estado == 17 & base_cp$c_mnpio == 35] <- 33
base_cp$c_mnpio[base_cp$c_estado == 17 & base_cp$c_mnpio == 36] <- 33

base_cp$c_mnpio[base_cp$c_estado == 23 & base_cp$c_mnpio == 11] <- 10




faltantes <- base_coor %>% filter(is.na(Estado)) %>% select(CODIGO) %>% 
  unique() %>% unlist()



for (i in 1:length(faltantes)) {
  
  auxiliar <- as.integer(faltantes[i] / 100) * 100
  
  est_sus <- base_coor$Estado[base_coor$CODIGO == auxiliar][1]
  mun_sus <- base_coor$Mun[base_coor$CODIGO == auxiliar][1]
  
  base_coor$Estado[base_coor$CODIGO == faltantes[i]] <- est_sus
  base_coor$Mun[base_coor$CODIGO == faltantes[i]] <- mun_sus
  
  
}



cont_estado <- base_coor %>% na.omit() %>% select(Estado, Mun) %>% 
  group_by(Estado) %>% summarise(Mun = n())


mapa$cuenta <- NA

for (i in 1:nrow(cont_estado)) {
  mapa$cuenta[as.numeric(mapa$cvegeoedo) == i] <- cont_estado$Mun[i]
  
}


mapa$cc <- cut(mapa$cuenta, breaks = c(0, 200, 400, 800, 1000, 1200,
                                       Inf), 
               labels = c('< 200','200 - 400', '400 - 800,', 
                          '800 - 1000', '1000 - 1200', '> 1200'))


ggplot() + geom_sf( aes(fill = cc), data = mapa) + theme_bw() +
  geom_sf(fill = 'transparent', col = 'lightgray', data = mapa) +
  scale_fill_viridis_d(option = 'magma', direction = -1) +
  theme(legend.position = 'bottom')






## tiendas de abarrotes


sc <- spark_connect(master = "local")

base_ab <- spark_read_csv(sc, path = "denue_inegi_46111_.csv")


base_coord_ab <- base_ab %>% select(id,per_ocu,cve_ent,cve_mun,cod_postal,latitud,longitud) %>% 
  collect()

spark_disconnect(sc)


cont_abarr <- base_coord_ab %>% select(cve_ent, id) %>% 
  group_by(cve_ent) %>% summarise(id = n(), .groups = 'drop')



mapa$cuenta_ab <- NA

for (i in 1:nrow(cont_abarr)) {
  mapa$cuenta_ab[as.numeric(mapa$cvegeoedo) == i] <- cont_abarr$id[i]
  
}

mapa$cc_ab <- cut(mapa$cuenta_ab, breaks = c(0, 5000, 10000, 20000, 30000, 40000, 60000,
                                             Inf), 
                  labels = c('< 5k','5k - 10k', '10k - 20k,', 
                             '20k - 30k', '30k - 40k', '40k - 60k','> 60k'))



ggplot() + geom_sf( aes(fill = cc_ab), data = mapa) + theme_bw() +
  geom_sf(fill = 'transparent', col = 'lightgray', data = mapa) +
  scale_fill_viridis_d(option = 'magma', direction = -1) +
  theme(legend.position = 'bottom')




#Nuevo León 19, Oaxaca 20, Estado de México 15, CDMX 9

co_nl <- base_coor %>% filter(Estado == 19) %>% select(Estado, Mun, CODIGO) %>% 
  group_by(Estado, Mun) %>% summarise(CODIGO = n(), .groups = 'drop')


map_nl <- mapa_mun %>% filter(cvegeoedo == '19')

map_nl$cuo <- 0

for (i in 1:nrow(co_nl)) {
  map_nl$cuo[as.numeric(map_nl$cve_mun) == co_nl$Mun[i]] <- co_nl$CODIGO[i]
  
  
}


map_nl$cuo_c <- cut(map_nl$cuo, breaks = c(-1e-10, 10, 20, 40, 60, 80, 100, 200, Inf), 
                    labels = c('< 10','10 - 20', '20 - 40', 
                               '40 - 60', '60 - 80', '80 - 100', '100 - 200', '> 200'))


ggplot() + geom_sf( aes(fill = cuo_c), data = map_nl) + theme_bw() +
  geom_sf(fill = 'transparent', col = 'lightgray', data = map_nl) +
  scale_fill_viridis_d(option = 'magma', direction = -1, name = 'Numero de Tiendas Oxxo') +
  theme(legend.position = 'bottom')




ca_nl <- base_coord_ab %>% filter(cve_ent == 19) %>% select(cve_ent, cve_mun, cod_postal) %>% 
  group_by(cve_ent, cve_mun) %>% summarise(cod_postal = n(), .groups = 'drop')

map_nl$cua <- 0

for (i in 1:nrow(ca_nl)) {
  map_nl$cua[as.numeric(map_nl$cve_mun) == ca_nl$cve_mun[i]] <- ca_nl$cod_postal[i]
  
}


map_nl$cua_c <- cut(map_nl$cua, breaks = c(-1e-10, 50, 100, 200, 500, 1000, 2000, Inf), 
                    labels = c('< 50','50 - 100', '100 - 200', 
                               '200 - 500', '500 - 1000', '1000- 2000', '> 2000'))


ggplot() + geom_sf( aes(fill = cua_c), data = map_nl) + theme_bw() +
  geom_sf(fill = 'transparent', col = 'lightgray', data = map_nl) +
  scale_fill_viridis_d(option = 'magma', direction = -1, name = 'Numero de Tiendas abarrotes') +
  theme(legend.position = 'bottom')






co_oa <- base_coor %>% filter(Estado == 20) %>% select(Estado, Mun, CODIGO) %>% 
  group_by(Estado, Mun) %>% summarise(CODIGO = n(), .groups = 'drop')

map_oa <- mapa_mun %>% filter(cvegeoedo == '20')

map_oa$cuo <- 0

for (i in 1:nrow(co_oa)) {
  map_oa$cuo[as.numeric(map_oa$cve_mun) == co_oa$Mun[i]] <- co_oa$CODIGO[i]
}


map_oa$cuo_c <- cut(map_oa$cuo, breaks = c(-1e-10, 5, 10, 15, 20, 25, 30, Inf), 
                    labels = c('< 5','5 - 10', '10 - 15', 
                               '15 - 20', '20 - 25', '25 - 30', '> 30'))


ggplot() + geom_sf( aes(fill = cuo_c), data = map_oa) + theme_bw() +
  geom_sf(fill = 'transparent', col = 'lightgray', data = map_oa) +
  scale_fill_viridis_d(option = 'magma', direction = -1, name = 'Numero de Tiendas Oxxo') +
  theme(legend.position = 'bottom')







ca_oa <- base_coord_ab %>% filter(cve_ent == 20) %>% select(cve_ent, cve_mun, cod_postal) %>% 
  group_by(cve_ent, cve_mun) %>% summarise(cod_postal = n(), .groups = 'drop')

map_oa$cua <- 0

for (i in 1:nrow(ca_oa)) {
  map_oa$cua[as.numeric(map_oa$cve_mun) == ca_oa$cve_mun[i]] <- ca_oa$cod_postal[i]
  
  
}

map_oa$cua_c <- cut(map_oa$cua, breaks = c(-1e-10, 20, 40, 60, 80, 100, 150, 
                                           200, 300, 400, 600, 1000, 2000, Inf), 
                    labels = c('< 20','20 - 40', '40 - 60', 
                               '60 - 80', '80 - 100', '100 - 150', '150 - 200',
                               '200 - 300','300 - 400', '400 - 600', '600 - 1000',
                               '1000 - 2000', '> 2000'))


ggplot() + geom_sf( aes(fill = cua_c), data = map_oa) + theme_bw() +
  geom_sf(fill = 'transparent', col = 'lightgray', data = map_oa) +
  scale_fill_viridis_d(option = 'magma', direction = -1, name = 'Numero de Tiendas de abarrotes') +
  theme(legend.position = 'bottom')







map_mx <- mapa_mun %>% filter(as.numeric(cvegeoedo) %in% c(9,15))

co_mx <- base_coor %>% filter(Estado %in% c(9,15)) %>% select(Estado, Mun,CODIGO) %>% 
  group_by(Estado, Mun) %>% summarise(CODIGO = n(), .groups = 'drop')


map_mx$cuo <- 0

for (i in 1:nrow(co_mx)) {
  map_mx$cuo[as.numeric(map_mx$cve_mun) == co_mx$Mun[i] & 
               as.numeric(map_mx$cvegeoedo) == co_mx$Estado[i]] <- co_mx$CODIGO[i]
  
}


map_mx$cuo_c <- cut(map_mx$cuo, breaks = c(-1e-10, 10, 20, 40, 60, 80, 100, 150, 200, Inf), 
                    labels = c('< 10','10 - 20', '20 - 40', 
                               '40 - 60', '60 - 80', '80 - 100', '100 - 150',
                               '150 - 200', '> 200'))


ggplot() + geom_sf( aes(fill = cuo_c), data = map_mx) + theme_bw() +
  geom_sf(fill = 'transparent', col = 'lightgray', data = map_mx) +
  scale_fill_viridis_d(option = 'magma', direction = -1, name = '') +
  theme(legend.position = 'bottom') + annotation_scale() + annotation_north_arrow(location = "tr") 
  



  ca_mx <- base_coord_ab %>% filter(cve_ent %in% c(9,15)) %>% select(cve_ent, cve_mun, cod_postal) %>% 
  group_by(cve_ent, cve_mun) %>% summarise(cod_postal = n(), .groups = 'drop')

map_mx$cua <- 0

for (i in 1:nrow(ca_mx)) {
  map_mx$cua[as.numeric(map_mx$cve_mun) == ca_mx$cve_mun[i] & 
               as.numeric(map_mx$cvegeoedo) == ca_mx$cve_ent[i]] <- ca_mx$cod_postal[i]
  
}

map_mx$cua_c <- cut(map_mx$cua, breaks = c(-1e-10,100, 200, 300, 400, 
                                           500, 700, 1000, 1500, 2000, 3000, Inf), 
                    labels = c('< 100','100 - 200', 
                              '200 - 300', '300 - 400', '400 - 500',
                               '500 - 700','700 - 1000', '1000 - 1500', '1500 - 2000',
                               '2000 - 3000', '> 3000'))


ggplot() + geom_sf( aes(fill = cua_c), data = map_mx) + theme_bw() +
  geom_sf(fill = 'transparent', col = 'lightgray', data = map_mx) +
  scale_fill_viridis_d(option = 'magma', direction = -1, name = '') +
  theme(legend.position = 'bottom')+ annotation_scale() + annotation_north_arrow(location = "tr")




## comienza la medición, función validada 

distancia <- function(x1, y1, x2, y2){
  
  dlat <- (y2 - y1) * (pi / 180)
  dlon <- (x2 - x1) * (pi / 180)
  r_tierra <- 6371
  
  a <- sin(dlat / 2) * sin(dlat / 2) + cos(y1 * pi / 180) * cos(y2 * pi / 180) *
    sin(dlon / 2) * sin(dlon / 2)
  
  c <- 2 * atan2(sqrt(a), sqrt(1-a))
  
  d <- c * r_tierra
  return(d)
  
  
}





vector_co <- base_coord_ab %>% filter(cve_ent %in% c(9, 15, 19)) %>% arrange(cod_postal)

con_dis <- vector_co %>% select(cve_ent, cve_mun) %>% distinct() %>% 
  arrange(cve_ent, cve_mun)

vector_co$cont <- 0



for (i in 1:nrow(con_dis)) {
  aux_aba <- vector_co %>% filter(cve_ent == con_dis$cve_ent[i],
                                  cve_mun == con_dis$cve_mun[i])
  
  aux_oxo <- base_coor %>% filter(Estado == con_dis$cve_ent[i],
                                  Mun == con_dis$cve_mun[i])
  
  for (j in 1:nrow(aux_aba)) {
    vec_dis <- distancia(x1 = aux_aba$longitud[j], y1 = aux_aba$latitud[j],
                         x2 = aux_oxo$LONGITUD, y2 = aux_oxo$LATITUD)
    
    aux_con <- length(vec_dis[vec_dis <= .350])
      
    if(aux_con > 0){
      
      vector_co$cont[vector_co$id == aux_aba$id[j]] <- aux_con
      
    }else{
      
      vector_co$cont[vector_co$id == aux_aba$id[j]] <- 0
      
    }
    
  }
  
}


abar_col <- vector_co %>% filter(cont > 0) %>% select(id) %>% unlist()



co_oxo <- base_coor %>% filter(Estado %in% c(9,15), LONGITUD < 0)
co_oxo$cont <- 0

base_aba_col <- base_coord_ab %>% filter(id %in% abar_col)

for (i in 1:nrow(co_oxo)) {
  aba_aux <- base_aba_col %>% filter(cve_ent == co_oxo$Estado[i],
                                     cve_mun == co_oxo$Mun[i])
  
  dis_vec <- distancia(x1 = co_oxo$LONGITUD[i], y1 = co_oxo$LATITUD[i],
                       x2 = aba_aux$longitud, y2 =aba_aux$latitud)
  
  co_oxo$cont[i] <- dis_vec[dis_vec <= .350] %>% length()
}




## adaptar codigo siguiente
c_mun_est <- co_oxo %>% select(Estado, Mun, cont) %>% filter(cont > 0) %>% 
  group_by(Estado, Mun) %>% summarise(num_tiend = mean(cont), .groups = 'drop') %>% 
  arrange(desc(num_tiend))

c_mun_oxxo <- base_coor %>% filter(Estado %in% c(9,15)) %>% select(Estado, Mun, CODIGO) %>%
  group_by(Estado, Mun) %>% summarise(CODIGO = n(), .groups = 'drop') %>% 
  arrange(desc(CODIGO))

## mapa completo

mapa_bj <- mapa_mun %>% filter(cvegeoedo == '15', cve_mun == '122')


oxxo_bj <- base_coor %>% filter(Estado == 15, Mun == 122)
abar_bj <- base_coord_ab %>% filter(cve_ent == 15, cve_mun == 122)


ggplot() + geom_sf(data = mapa_bj) + 
  geom_point(data = oxxo_bj, aes(x = LONGITUD, y = LATITUD, col = 'red')) +
    geom_point(data = abar_bj, aes(x = longitud, y = latitud, col = 'blue'), size = 0.1) + 
      theme_bw() + annotation_scale() + annotation_north_arrow(location = "tr") +
  scale_color_manual(name = 'Tipo de comercio', values = c('red'='red','blue'='blue'), 
                     labels = c('Abarrotes', 'OXXO'))


## mapa con las tiendas en unradio de 350 mts 
abar_rmap <- abar_bj %>% filter(id %in% abar_col)



ggplot() + geom_sf(data = mapa_bj) + 
  geom_point(data = oxxo_bj, aes(x = LONGITUD, y = LATITUD, col = 'red')) +
  geom_point(data = abar_rmap, aes(x = longitud, y = latitud, col = 'blue'), size = 0.1) + 
  theme_bw() + annotation_scale() + annotation_north_arrow(location = "tr") +
  scale_color_manual(name = 'Tipo de comercio', values = c('red'='red','blue'='blue'), 
                     labels = c('Abarrotes', 'OXXO')) + 
  theme(legend.position = 'bottom')
#geom_circle(data = oxxo_bj,aes(x0 = LONGITUD, y0 = LATITUD, r =.0035), lwd = .2, linetype = 'dotted') + 





