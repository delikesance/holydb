    Fonctionnement du CORE

Chaque donnée est enregistrée dans un fichier qu'on appelle "le disque dur". Ce "disque dur" fonctionnement
exactement comme un réel disque dur. Chaque donnée est enregistrée dans le fichier "disque dur" puis référencé
dans le fichier d'index.

Le fichier d'index spécifie, un ID (une clef), le hash de la donnée, la position de départ dans le disque dur
et la taille de la donnée.

Pour la recherche par ID il est donc simple d'accéder a la donnée puisqu'il suffit de trouver l'index qui
correspond, lire le disque dur a la position de départ et donc lire le nombre de bytes spécifiés.

Pareil pour la recherche par donnée exacte, il suffit de hash (déterministe) la donnée qu'on cherche,
rechercher le hash correspondant dans l'index, puis faire le meme process de lecture que plus haut.

[] TODO: Réfléchir a la recherche par "mot clef".

