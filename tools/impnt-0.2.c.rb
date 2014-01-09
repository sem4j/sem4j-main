REST_URL = 'http://localhost:7474/'
#REST_URL = 'http://07354a721:f40e047e0@a70d19d29.hosted.neo4j.org:7573/'
HEADER = { 'Content-Type' => 'application/json' }

%w{rubygems json cgi faraday}.each{|r| require r}


# make a connection to the Neo4j REST server
conn = Faraday.new(:url => REST_URL) do |builder|
  builder.adapter :net_http
end

#conn.options.proxy = {
#  :uri => 'http://a70d19d29.hosted.neo4j.org:7573/',
#  :user => '07354a721',
#  :password => 'f40e047e0' 
#}

# GET OR CREATE NODE
def get_or_create_node(conn, uri)
  # PARSE URI
  _, namespace, name = uri.split(/^(.*[\/|#])([^\/|#]+)$/)
  # LOOK FOR THIS NODE IN THE INDEX
  r = conn.get("/db/data/index/node/#{CGI.escape(namespace)}/name/#{CGI.escape(name)}")
  node = (JSON.parse(r.body).first || {})['self'] if r.status == 200
  #puts r.status, node
  unless node
    # THIS NODE IS NOT FOUND IN THE INDEX, SO CREATE IT
    r = conn.post("/db/data/node", JSON.unparse({:name => name, :uri => uri}), HEADER)
    node = (JSON.parse(r.body) || {})['self'] if [200, 201].include? r.status
    # NAMESPACE INDEX
    data1 = {:uri => node, :key => "name", :value => CGI.escape(name)}
    conn.post("/db/data/index/node/#{CGI.escape(namespace)}", JSON.unparse(data1), HEADER)
    # FILE INDEX
    data2 = {:uri => node, :key => "file", :value => $file_name}
    conn.post("/db/data/index/node/files", JSON.unparse(data2), HEADER)
    $load_n += 1
  else
    $skip_n += 1
  end
  # RETURN NEO4J'S NODE OBJECT
  node
end

# ADD A PROPERTY ON A NODE
def add_property(conn, node, value, type)
  # Check if the same property is already added
  r = conn.get("#{node}/properties/#{CGI.escape(type)}")
  #p r.status
  #p r.body, value
  #puts JSON.parse(value_stored.body)['exception']
  if r.status != 200
    conn.put("#{node}/properties/#{CGI.escape(type)}", "#{value.match(/"(.*)"/)}", HEADER) 
    $load_p += 1
  else
    if r.body == value
      $skip_p += 1
    else
      puts "WARNING (Property Conflict): #{node}/properties/#{CGI.escape(type)} old: #{r.body} <> new: #{value}"
      $warn_p += 1
    end
  end
  #puts "#{node}/properties/#{CGI.escape(type)}"
end

# method to get existing relationship using traversal from its start node, or create one
def get_or_create_relationship(conn, node_start, node_end, type)
  data = <<"EOS"
{
  "to" : "#{node_end}",
  "max_depth" : 1,
  "relationships" : {
    "type" : "#{type}",
    "direction" : "out"
  },
  "algorithm" : "shortestPath"
}
EOS
  r = conn.post("#{node_start}/paths", data, HEADER)
  length = (JSON.parse(r.body).first || {})['length'] if r.status == 200
  if length != 1
    # THIS NODE IS NOT FOUND IN THE INDEX, SO CREATE IT
    r = conn.post("#{node_start}/relationships", JSON.unparse({ :to => node_end, :type => type }), HEADER)
    relationship = (JSON.parse(r.body) || {})['self'] if [200, 201].include? r.status
    #puts r.status
    #puts relationship
    # FILE INDEX
    r_data = "{\"uri\" : \"#{relationship}\", \"key\" : \"file\", \"value\" : \"#{$file_name}\"}"
    r2 = conn.post("/db/data/index/relationship/files", r_data, HEADER)
    $load_r += 1
  else
    $skip_r += 1
  end
end

puts "begin processing..."

count = 0
$load_n = 0
$skip_n = 0
$load_r = 0
$skip_r = 0
$load_p = 0
$skip_p = 0
$warn_p = 0
$file_name = ""

File.open(ARGV[0]).each do |line|
  _, $file_name = ARGV[0].split(/([^\/]+)$/)
  uri_s, uri_p, uri_o_tmp = line.delete("<").delete(">").split(nil, 3)
  
  # WHEN URI_O IS A LITERAL
  if m = uri_o_tmp.match(/(".*").*/)
    uri_o = m[1]
  # WHEN URI_O IS A URI
  else
    uri_o = uri_o_tmp.split(nil)[0]
  end

  # GO TO THE NEXT TRIPLE IF EMPTY
  next if uri_s.empty? || uri_p.empty? || uri_o.empty?
  
  # CREATE A NODE (OR JUST GET NODE ID) FOR URI_S
  node_s = get_or_create_node(conn, uri_s)
  
  # WHEN URI_O IS A LITERAL, ADD A PROPERTY
  if value_o = uri_o.match(/"(.*)".*/)
    add_property(conn, node_s, uri_o, uri_p)
  # WHEN URI_O IS A URI, CREATE A NODE FOR URI_O AND A RELATIONSHIP
  else
    node_o = get_or_create_node(conn, uri_o)
    get_or_create_relationship(conn, node_s, node_o, uri_p)
  end

  puts "  #{count} triples loaded" if (count += 1) % 100 == 0
end

puts "  #{$load_n} nodes loaded"
puts "  #{$skip_n} nodes skipped"
puts "  #{$load_r} relationships loaded"
puts "  #{$skip_r} relationships skipped"
puts "  #{$load_p} properties loaded"
puts "  #{$skip_p} properties skipped"
puts "  #{$warn_p} properties warning"

puts "done!"
