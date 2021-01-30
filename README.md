# Projekat iz predmeta Arhitekture sistema velikih skupova podataka

Tema projektnog zadatka je analiza kriminalnih prekršaja na teritoji New York-a

# Motivacija
  
  Kako se kriminal promenio tokom godina?\
  Da li je moguće predvideti gde i kada će biti počinjen sledeći zločin?\
  Koji delovi grada su napredovali tokom vremenskog perioda?

# Dataset

  Link: https://www.kaggle.com/mrmorj/new-york-city-police-crime-data-historic
  
  Sadrži podatke svih prijavljenih slučajeva zločina u New York-u tokom određenog vremenskog perioda.\
  Skup podataka sadrži nešto manje od 7m slučajeva\
  Format podataka se nalazi u csv formatu

   ### Sadrzaj
  -ID\
  -Policijska stanica\
  -Opština\
  -Vreme kada se zločin dogodio\
  -Kod za klasifikaciju prekršaja\
  -Tačan opis lokacije\
  -Podaci o osumljičenom\
  -Podaci o žrtvi\
  -X,Y kordinate lokacije geografska dužina i širina

  # Batch obrada
   -Ukupan broj prekršaja po godini\
   -Ukupan broj prekšaja po opštini\
   -Najčešći zločini po godini, opštini,...\
   -Vremenski period kade se desilo najviše prekršaja\
   -Koje vrste prekršaja su najzastupljenije po godini, opštini,...\
   -Odnos ukupnoog stanovništva i prekršaja po regiji\
   -Prosečni broj godina osumljičenog i žrtve
   
  # Stream obrada
   -Simulacija obrade podataka u trenutku kada su kreirani\
   -Kategorizacija prekršaja (prikaz notifikacije ako se desio određeni zločin)



