# TEST

# tab2graph
python tab2graph.py - test/tab2graph_input.dat test/tab2graph_output_n.dat test/tab2graph_output_r.dat \
'{"id":__1__, "properties":[{"name":"sex", "value":__2__}]},{"id":__3__, "properties":[{"name":"tel", "value":__4__}]}' \
'{"source":__1__, "target":__3__, "properties":[{"name":"type", "value":__5__}, {"name":"years", "value":__6__}]}'

# graph2db
ruby graph2db.rb http://localhost:7474/ test/graph2db_input_n.dat test/graph2db_input_r.dat > test/graph2db_output.dat
