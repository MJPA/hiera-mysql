# Class Mysql_backend
# Description: MySQL back end to Hiera.
# Author: Craig Dunn <craig@craigdunn.org>
#
class Hiera
    module Backend
        class Mysql_backend
            def initialize
                begin
                  require 'mysql'
                rescue LoadError
                  require 'rubygems'
                  require 'mysql'
                end

                Hiera.debug("mysql_backend initialized")
            end
            def lookup(key, scope, order_override, resolution_type)

                Hiera.debug("mysql_backend invoked lookup")
                Hiera.debug("resolution type is #{resolution_type}")

                answer = nil

                # Parse the mysql query from the config, we also pass in key
                # to extra_data so this can be interpreted into the query
                # string
                #
                queries_config = Config[:mysql][:query]
                if queries_config.is_a? Hash
                  queries = []
                  lookup_pair = key.split(':', 2)
                  if lookup_pair.length != 2
                    lookup_pair = [ '', lookup_pair[0].to_s ]
                  else
                    lookup_pair[0] = lookup_pair[0].to_s
                  end

                  queries_config.each_pair do |query_key, query_sql|
                    if lookup_pair[0] == query_key.to_s or lookup_pair[0] == ''
                      queries.push Backend.parse_string(query_sql, scope, {"key" => lookup_pair[1] })
                    end
                  end
                else
                  queries = [ queries_config ].flatten
                  queries.map! { |q| Backend.parse_string(q, scope, {"key" => key}) }
                end

                queries.each do |mysql_query|

                  results = query(mysql_query)

                  unless results.empty?
                    case resolution_type
                      when :array
                        answer ||= []
                        results.each do |ritem|
                          answer << Backend.parse_answer(ritem, scope)
                        end
                      else
                       answer = Backend.parse_answer(results[0], scope)
                       break
                    end
                  end

                end
              answer
            end

            def query (sql)
                Hiera.debug("Executing SQL Query: #{sql}")

                data=[]
                mysql_host=Config[:mysql][:host]
                mysql_user=Config[:mysql][:user]
                mysql_pass=Config[:mysql][:pass]
                mysql_database=Config[:mysql][:database]

                dbh = Mysql.new(mysql_host, mysql_user, mysql_pass, mysql_database)
                dbh.reconnect = true

                res = dbh.query(sql)
                Hiera.debug("Mysql Query returned #{res.num_rows} rows")


                # Currently we'll just return the first element of each row, a future
                # enhancement would be to make this easily support hashes so you can do
                # select foo,bar from table
                #
                if res.num_fields < 2
                  res.each do |row|
                    Hiera.debug("Mysql value : #{row[0]}")
                    data << row[0]
                  end

                else
                  res.each_hash do |row|
                    data << row
                  end
                end

                return data
            end
        end
    end
end


