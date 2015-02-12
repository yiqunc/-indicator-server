package au.edu.csdila.indicator;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import au.edu.csdila.common.*;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.commons.dbcp.BasicDataSource;

public class DbAccess {

	static final Logger LOGGER = LoggerFactory.getLogger(DbAccess.class);
	
	private static Context initCtx = null;
    private static Context envCtx = null;
    private static BasicDataSource pgds = null;
    
    /**
     * Init Postgres Data source
     */
	public static void initPostgresDataSource(){
		
		if(pgds!=null) return;
		
		try {
			initCtx = new InitialContext();
			envCtx = (Context) initCtx.lookup("java:comp/env");
			pgds = (BasicDataSource)envCtx.lookup("jdbc/pgds_basic");
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Get job(sim) records for specific type of analysis of specific login user 
	 * @param maxNum
	 * @param modelId
	 * @param userId
	 * @return a JSON array containing the results.   
	 */
	public static JSONArray getSimRecords(int maxNum, int modelId, int userId){
		
		//TODO :check options if valid
		if (maxNum<=0) maxNum = 10;
		
		JSONArray results = new JSONArray();
		Connection conn = null;
	    PreparedStatement pst = null;
	    ResultSet rs = null;

		try {
			
			 conn = pgds.getConnection();
			 pst = conn.prepareStatement("SELECT * FROM tbl_sim WHERE modelid = ? and userid = ? and simstate=2 ORDER BY simid DESC LIMIT ?;");
			 
			 pst.setInt(1, modelId);
			 pst.setInt(2, userId);
			 pst.setInt(3, maxNum);
			 
			 rs = pst.executeQuery();
			 
			 while(rs!=null && rs.next()){
				 JSONObject sim = new JSONObject();
				 sim.put("simid", rs.getInt("simid"));
				 sim.put("simcode", "job_"+rs.getInt("simid"));
				 sim.put("data", new JSONObject(rs.getString("simoutputs")));
				 sim.put("para", new JSONObject(rs.getString("simoptions")));
				 results.put(sim);
			 }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			
			try {
                if (rs != null) {
                    rs.close();
                }
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                
            }
		}

		return results;
	}
	
	/**
	 * Get specific output data for a specific simid
	 * @param simId
	 * @param columnName: currently can be either 'simoutputs' or 'simextraoutputs'
	 * @return a JSON object containing the output data
	 */
	public static JSONObject getSimOutputs(int simId, String columnName){
		
		JSONObject result = null;
		Connection conn = null;
	    PreparedStatement pst = null;
	    ResultSet rs = null;

		try {
			
			 conn = pgds.getConnection();
			
			 pst = conn.prepareStatement("SELECT "+columnName+" FROM tbl_sim WHERE simid = ?;");
			 
			 pst.setInt(1, simId);
			 
			 rs = pst.executeQuery();
			 
			 while(rs!=null && rs.next()){

				 result = new JSONObject(rs.getString(columnName));
				
			 }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			
			try {
                if (rs != null) {
                    rs.close();
                }
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                
            }
		}

		return result;
	}
	
	/**
	 * Get job(sim) input params for a specific simid
	 * @param simId
	 * @return a JSON object containing the input params data
	 */
	public static JSONObject getSimOptions(int simId){
		
		JSONObject options = null;
		Connection conn = null;
	    PreparedStatement pst = null;
	    ResultSet rs = null;

		try {
			
			 conn = pgds.getConnection();
			
			 pst = conn.prepareStatement("SELECT simoptions FROM tbl_sim WHERE simid = ?;");
			 
			 pst.setInt(1, simId);
			 
			 rs = pst.executeQuery();
			 
			 while(rs!=null && rs.next()){

				 options = new JSONObject(rs.getString("simoptions"));
				
			 }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			
			try {
                if (rs != null) {
                    rs.close();
                }
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                
            }
		}

		return options;
	}

	/**
	 * Initialize a job(sim) record based on client defined params  
	 * @param options: client defined params 
	 * @return the newly create job(sim) id.   
	 */
	public static int createSim(JSONObject options){
		
		//TODO :check options if valid
		
		Connection conn = null;
	    PreparedStatement pst = null;
	    ResultSet rs = null;
	    int newSimId =  -1;
		try {
			
			 conn = pgds.getConnection();
			
			 pst = conn.prepareStatement("INSERT INTO tbl_sim(userid, modelid, simoptions) VALUES (?, ?, ?) returning simid;");
			 
			 pst.setInt(1, options.getInt("userid"));
			 pst.setInt(2, options.getInt("modelid"));
			 pst.setString(3, options.getString("options"));
			 rs = pst.executeQuery();
			
			 if(rs!=null && rs.next()){
				 LOGGER.info("==== new inserted simid:{}", rs.getInt("simid"));
				 newSimId = rs.getInt("simid");
			 }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			
			try {
                if (rs != null) {
                    rs.close();
                }
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                
            }
		}

		return newSimId;
	}
	
	/**
	 * Update the processing state of a job(sim) for a specific simid
	 * @param simid
	 * @param state
	 * @return success or not
	 */
    public static boolean updateSimState(int simid, int state){
		
		//TODO :check options if valid
		
		Connection conn = null;
	    PreparedStatement pst = null;
	    boolean flag= false;
		try {
			
			 conn = pgds.getConnection();
			
			 pst = conn.prepareStatement("update tbl_sim set simstate=? where simid = ?");
			 
			 pst.setInt(1, state);
			 pst.setInt(2,simid);
			 pst.execute();
			 
			 flag = true;
			 
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			
			try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                
            }
			finally{
				
			}
		}

		return flag;
	}
    
	/**
	 * Update specific output data for a specific simid
	 * @param simid
	 * @param outputs
	 * @param columnName: currently can be either 'simoutputs' or 'simextraoutputs'
	 * @return success or not
	 */
	public static boolean updateSimResult(int simid, Object outputs, String columnName){
		
		//TODO :check options if valid
		
		Connection conn = null;
	    PreparedStatement pst = null;
	    boolean flag= false;
		try {
			
			 conn = pgds.getConnection();
			
			 pst = conn.prepareStatement("update tbl_sim set "+columnName+"=? where simid = ?");
			 
			 pst.setString(1, outputs.toString());
			 pst.setInt(2,simid);
			 pst.execute();
			 
			 flag = true;
			 
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			
			try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                
            }
			finally{
				
			}
		}

		return flag;
	}
	
	/**
	 * Check if user login is successful or not
	 * @param username
	 * @param pswd
	 * @return a JSON object containing user info. 
	 */
	public static JSONObject userLogin(String username, String pswd){
		
		
		JSONObject result = new JSONObject();
		Connection conn = null;
	    PreparedStatement pst = null;
	    ResultSet rs = null;
	    
		try {
			
		    result.put("userid", -1);
		    result.put("username", "");
			
			 conn = pgds.getConnection();
			
			 pst = conn.prepareStatement("SELECT userid, username FROM tbl_user WHERE username = ? and userpswd = ?;");
			 
			 pst.setString(1, username);
			 pst.setString(2, pswd);
			 
			 rs = pst.executeQuery();
			 
			 while(rs!=null && rs.next()){
				 
				 result.put("userid", rs.getInt("userid"));
				 result.put("username", rs.getString("username"));
				
			 }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			
			try {
                if (rs != null) {
                    rs.close();
                }
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                
            }
		}

		return result;
	}
}
