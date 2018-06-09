solrctl instancedir --generate $HOME/indexer_demo_fix
solrctl instancedir --create indexer_demo_fix $HOME/indexer_demo_fix
solrctl collection --create indexer_demo_fix -s 1 -r 2
