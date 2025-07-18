COMPOSANTS PRINCIPEAUX

	STORAGE ENGINE : Gestion de segments de données
	
	- Ecritures séquentielles uniquement
	- Segments de taille fixe (256MB)
	- Compression transparante (LZ4)


	INDEX MANAGER : Accès rapide aux données

	- Hash table pour lookup O(1)
	- Bloom filter pour éviter les I/O inutiles
	- Cache LRU pour les données chaudes


	WAL (Write-Ahead Log) : Garantie de durabilité

	- Toutes les écritures sont d'abord loggées
	- Récupération automatique après crash
	- Synchronisation périodique

STRUCTURE DE FICHIERS

	ORGANISATION SUR DISQUE

	/data/core/
		segments/
			segment_000001.dat (256MB)
			segment_000002.dat (256MB)
			segment_XXXXXX.dat
		index/
			hash_index.idx     (index principal)
			bloom_filter.blm   (filtre bloom)
			metadata.meta      (métadonnées système)
		wal/
			wal_000001.log     (write-ahead log)
			wal_active.log     (WAL actif)


	FORMAT DES SEGMENTS

	Chaque segment commence par un header de 64 bytes :

	Offset  Size    Description
	------  ----    -----------
	0	4	Magic Number 
	4	4	Version du format (1)
	8	8	Segment ID unique
	16	8	Timestamp de création
	24	4	Nombre de records
	28	8	Taille totale des données
	36	4	Checksum du segment
	40	24	Réservé pour extensions futures

	Puis les records des données :

	Record Format (variable) : 
	- Size (4 bytes)  : Taille du record
	- Hash (8 bytes)  : Hash de la clé
	- Data (variable) : Données sérialisées

	
	FORMAT DE L'INDEX

	L'index principal est une hash table avec chaînage :

	Index Entry (32 bytes) :
	- Key Hash (8 bytes) : Hash de la clé
	- Segment ID (4 bytes) : ID du segment contenant
	- Offset (8 bytes) : Position dans le segment
	- Size (4 bytes) : Taille du record
	- Timestamp (8 bytes) : Timestamp de création

	Bucket structure :
	- Chaque bucket contient une liste d'entries
	- Load factor maintenu a 75% maximum
	- Redimensionnement automatique si nécessaire

ALGORITHME DE BASE

	ECRITURE DE DONNEES (PUT)

	ENTREE : clé, données
	SORTIE : succès/échec

	1. Calcul du hash de la clé
	   hash_key = SHA256(key)

	2. Sérialisation des données
	   serialized_data = serialize(data)

	3. Ecriture dans le WAL
	   wal_entry = {hash_key, serialized_data, timestamp}
	   wal.append(wal_entry)

	4. Recherche du segment actif
	   if current_segment.size + record_size > MAX_SEGMENT_SIZE:
	   	current_segment = create_new_segment()

	5. Ecriture dans le segment
	   offset = current_segment.append(record)

	6. Mise à jour de l'index
	   index_entry = {hash_key, segment_id, offset, size, timestamp}
	   index.insert(index_entry)

	7. Commit du WAL
	   wal.sync()

	Complexité : O(1) amortisé
