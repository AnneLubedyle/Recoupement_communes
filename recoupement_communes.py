# coding : utf-8
r'''
File: c:\Users\agent\VigicrueFlash\Inventaire_communes\recoupement_communes.py
Project: c:\Users\agent\VigicrueFlash\Inventaire_communes
Created Date: Septembre 2019
Author: Anne BELLEUDY
-----
HISTORY:
'''
import shapefile
from shapely.geometry import shape
from rtree import index
import pandas as pd

seuil = 0.85

ficcom_old = \
    'C:/Users/agent/VigicrueFlash/SIG_DATA/communes_apic_l93/'\
    'commune_rte500_l93.shp'
ficcom_new = \
    'C:/Users/agent/VigicrueFlash/SIG_DATA/COMMUNES-ADME_COG_2019/'\
    'COM_ADME_COG_2019.shp'
ficabo = \
    'C:/Users/agent/VigicrueFlash/Abonnements/ABONNES/201909/'\
    'abonnement_mairie_20190902015454.csv'

com_old = shapefile.Reader(ficcom_old)
com_new = shapefile.Reader(ficcom_new)

dict_old = {e[2]: i for i, e in enumerate(com_old.iterRecords())}
dict_new = {e[3]: i for i, e in enumerate(com_new.iterRecords())}

abo = pd.read_table(ficabo, sep=";", index_col=0, dtype='str')
abo.fillna('', inplace=True)

# Création d'indexes spatiaux
""" Permet de gagner du temps sur la recherche par localisation"""
idx_new = index.Index()
for i, s in enumerate(com_new.iterShapes()):
    idx_new.insert(i, s.bbox)

idx_old = index.Index()
for i, s in enumerate(com_old.iterShapes()):
    idx_old.insert(i, s.bbox)


# Insee présents en _old et pas en _new
""" 1ere étape : INSEE qui ont été supprimés entre les 2 bases
Pour chaque commune supprimée, on cherche la commune nouvelle qui contient
l'ancienne commune.
On fixe un seuil afin de s'affranchir des différences de résolution sur les shp
Serie pandas : index = old et valeur = new
"""
old_to_new = pd.Series()
diff_old = dict_old.keys()-dict_new.keys()
for insee in list(diff_old):
    old_to_new[insee] = None
    s_old = shape(com_old.shape(dict_old[insee]))
    for i in idx_new.intersection(s_old.bounds):
        s_new = shape(com_new.shape(i))
        inter = s_old.intersection(s_new)
        if inter.area/s_old.area > seuil:
            old_to_new[insee] = com_new.record(i)[3]
            break

""" 2è étape
Pour chaque com nouvelle, on vérifie si le territoire de la commune ancienne
ayant le même INSEE est inclus dans le territoire de la commune nouvelle
"""
for insee in old_to_new.unique():
    try:
        s_old = shape(com_old.shape(dict_old[insee]))
        s_new = shape(com_new.shape(dict_new[insee]))
        inter = s_old.intersection(s_new)
        if inter.area/s_old.area > seuil:
            old_to_new[insee] = insee
        else:
            print(insee+' insee origine non trouvée')
    except KeyError:
        print(str(insee) + ' erreur clé')

""" 3è étape : récupération de l'info sur les abonnements"""
""" 3.1 on récupère les abonnements des communes anciennes"""
old_to_new = pd.concat([
    old_to_new.rename("Insee_new"),
    abo.reindex(
        old_to_new.index,
        columns=['Abonnement_apic', 'Abonnement_apoc']).fillna('')],
    axis=1, sort=False)
""" 3.2 On cherche les abonnements APIC impactés
Ne concerne pas Vigicrues Flash car il n'est possible de s'abonner que à sa
commune"""
old_to_new['Abonnements_impactes'] = ""
for insee in old_to_new.index:
    try:
        comabo = \
            abo.loc[abo.Abonnement_apic.str.contains(insee)].index.tolist()
        if comabo:
            old_to_new.loc[insee, 'Abonnements_impactes'] = ','.join(comabo)
    except:
        raise

"Tri de la table et export CSV"
old_to_new.sort_values(by='Insee_new', inplace=True)
old_to_new.to_csv('oldtonew.csv', index_label='Insee_old', sep=";")

# Bilan par communes nouvelles
group_by_com = old_to_new.groupby('Insee_new')
count_by_com = group_by_com.agg(
    {'Insee_new': 'size',  # Nb de communes anciennes dans la commune nouvelle
    'Abonnement_apic': lambda c: sum(c > ''),
    # Nombre de communes anciennes ayant au moins un abonnement APIC
    'Abonnement_apoc': lambda c: sum(c > ''),
    # Nombre de communes anciennes ayant au moins un abonnement Vigicrues Flash
    'Abonnements_impactes': lambda c: len(set([item for elt in c for\
    item in elt.split(',') if item]))  # Nombre d'abonnements APIC impactés
    }).rename(columns={'Insee_new': 'nb_insee_old'})

count_by_com['com_principale_abo_APIC'] = [
    int(df[df.index == insee].Abonnement_apic > '')
    for insee, df in group_by_com]
count_by_com['nb_com_suppr_abo_APIC'] = [
    sum(df[df.index != insee].Abonnement_apic > '')
    for insee, df in group_by_com]

count_by_com['com_principale_abo_VF'] = [
    int(df[df.index == insee].Abonnement_apoc > '')
    for insee, df in group_by_com]
count_by_com['nb_com_suppr_abo_VF'] = [
    sum(df[df.index != insee].Abonnement_apoc > '')
    for insee, df in group_by_com]

count_by_com.sort_values(by='nb_insee_old', inplace=True)
count_by_com.to_csv('compte_par_commune.csv', index_label='Insee_new', sep=";")

# Insee présents en _new et pas en _old
new_to_old = pd.Series()
diff_new = dict_new.keys()-dict_old.keys()
for insee in diff_new:
    new_to_old[insee] = None
    s_new = shape(com_new.shape(dict_new[insee]))
    for i in idx_old.intersection(s_new.bounds):
        s_old = shape(com_old.shape(i))
        inter = s_new.intersection(s_old)
        if inter.area/s_new.area > seuil:
            new_to_old[insee] = com_old.record(i)[2]

new_to_old = pd.concat([
    new_to_old.rename("Insee_old"),
    abo[['Abonnement_apic', 'Abonnement_apoc']]],
    axis=1, sort=False)
new_to_old = new_to_old[new_to_old.Insee_old.notnull()]

new_to_old.sort_values(by='Insee_old', inplace=True)

new_to_old.to_csv('newtoold.csv', index_label='Insee_new', sep=";")


# Insee dont le territoire a changé
"!!! LONG"
diffterr = pd.DataFrame({"ratio_surf_old": [], "ratio_surf_new": []})
for insee in dict_old.keys() & dict_new.keys() - set(old_to_new.Insee_new) - \
        set(new_to_old.Insee_old):
    s_old = shape(com_old.shape(dict_old[insee]))
    s_new = shape(com_new.shape(dict_new[insee]))
    inter = s_old.intersection(s_new)
    area = inter.area
    if area / s_old.area < seuil or area / s_new.area < seuil:
        diffterr.loc[insee, 'ratio_surf_old'] = area / s_old.area * 100
        diffterr.loc[insee, 'ratio_surf_new'] = area / s_new.area * 100


diffterr.to_csv('diffterr.csv', sep=';', float_format='%.1f',
    index_label='Insee')
