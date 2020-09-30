package top.dianay.influxdb.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class InfluxDBQuery {
	
	private Logger log = LoggerFactory.getLogger(this.getClass());
	
	public static void main(String[] args) {
		for(int i = 0 ; i < 10;i++) {
			Map<String,String> sumFieldNames = new HashMap<String, String>();
			sumFieldNames.put("Ia", "Ia");
			List<String> findFieldNames = new ArrayList<String>();
			findFieldNames.add("Ia");
			findFieldNames.add("Ib");
			findFieldNames.add("Ic");
			List<Object> mpList = new ArrayList<>();
			mpList.add("mp001");
			mpList.add("mp002");
			mpList.add("mp003");
			InfluxDBQuery sql = InfluxDBQuery.getInstane("t");
			System.out.println(sql.bulid());
		}
	}

	private InfluxDBQuery() {};

	public static final String timeField = "time";
	public static final String timeZone = " tz('Asia/Shanghai') ";
	
	private String querySql = "";

	public static InfluxDBQuery getInstane(String measurement) {
		InfluxDBQuery query = new InfluxDBQuery();
		query.querySql += " from "+ measurement + " where ";
		return query;
	}
	
	public static InfluxDBQuery getInstane(String measurement,String policies) {
		InfluxDBQuery query = new InfluxDBQuery();
		query.querySql += " from "+ policies +"."+ measurement + " where ";
		return query;
	}
	
	public InfluxDBQuery searchField(List<String> fieldNames) {
		if(CollectionUtils.isNotEmpty(fieldNames)) {
			int fieldMaxIndex =  fieldNames.size()-1;
			for(int i = 0; i <= fieldMaxIndex;i++) {
				String fieldName = fieldNames.get(i);
				if(i == fieldMaxIndex) {
					querySql = fieldName+querySql;
				}
				else {
					querySql = ","+fieldName+querySql;
				}
			}
			querySql = "select "+querySql;
		}
		else {
			querySql = "select * "+querySql;
		}
		return this;
	}
	
	public InfluxDBQuery sumField(Map<String,String> sumFieldNamesMap) {
		if(sumFieldNamesMap != null && sumFieldNamesMap.size() > 0) {
			for(Map.Entry<String, String> sumEntry : sumFieldNamesMap.entrySet()) {
				String sumFieldName = sumEntry.getKey();
				String fieldNameAs = sumEntry.getValue();
				querySql = ",sum("+sumFieldName + ") as " + fieldNameAs + querySql;
			}
			querySql = querySql.substring(1, querySql.length());
			querySql = "select "+querySql;
		}
		return this;
	}
	
	public InfluxDBQuery in(String fieldName,Object[] valList) {
		if(valList != null && valList.length > 0) {
			if (!"where".equals(querySql.substring((querySql.length() - 6), querySql.length()).trim())) {
				querySql += " and ( ";
			}
			else {
				querySql += " ( ";
			}
			int maxValIndex = valList.length - 1;
			for(int i = 0; i <= maxValIndex ;i++) {
				Object val = valList[i];
				if(i == maxValIndex) {
					if(fieldName.indexOf(timeField) > -1) {
						querySql += fieldName +" = '"+val+"'" ;
					}
					else {
						if(val instanceof String) {
							querySql += fieldName +" = '"+val+"'" ;
						}
						else {
							querySql += fieldName +" = "+val ;
						}
					}
				}
				else {
					if(fieldName.indexOf(timeField) > -1) {
						querySql += fieldName +" = '"+val+"'" +" or ";
					}
					else {
						if(val instanceof String) {
							querySql += fieldName +" = '"+val+"'" +" or ";
						}
						else {
							querySql += fieldName +" = "+val +" or ";
						}
					}
				}
				
			}
			querySql += " ) ";
		}
		return this;
	}

	public InfluxDBQuery betweenDate(Object startDate, Object endDate) {
		if (!"where".equals(querySql.substring((querySql.length() - 6), querySql.length()).trim())) {
			querySql += " and ";
		}
		if (startDate instanceof String && endDate instanceof String) {
			if (((String)startDate).length() < 19 || ((String)endDate).length() < 19) {
				querySql += " time >= '" + startDate + "' and time <= '" + endDate + "'";
			} else {
				throw new RuntimeException(startDate + "--" + endDate + ".in come date formate is error!formateis : yyyy-MM-dd HH:mm:ss");
			}
		} else if (startDate instanceof Date && endDate instanceof Date) {
			String sd = DateFormatUtils.format((Date)startDate, "yyyy-MM-dd HH:mm:ss");
			String ed = DateFormatUtils.format((Date)endDate, "yyyy-MM-dd HH:mm:ss");
			querySql += " time >= '" + sd + "' and time <='" + ed + "' ";
		} else {
			throw new RuntimeException(startDate + "--" + endDate + ".in come date params is error!");
		}
		return this;
	}
	
	public InfluxDBQuery and(String fieldName,Object val) {
		if (!"where".equals(querySql.substring((querySql.length() - 6), querySql.length()).trim())) {
			querySql += " and ";
		}
		if(fieldName.indexOf(timeField) > -1) {
			querySql += fieldName +" = '"+val+"'" ;
		}
		else {
			if(val instanceof String) {
				querySql += fieldName +" = '"+val+"'" ;
			}
			else {
				querySql += fieldName +" = "+val ;
			}
		}
		return this;
	}
	
	public InfluxDBQuery or(String fieldName,Object val) {
		if (!"where".equals(querySql.substring((querySql.length() - 6), querySql.length()).trim())) {
			querySql += " or ";
		}
		if(fieldName.indexOf(timeField) > -1) {
			querySql += fieldName +" = '"+val+"'" ;
		}
		else {
			if(val instanceof String) {
				querySql += fieldName +" = '"+val+"'" ;
			}
			else {
				querySql += fieldName +" = "+val ;
			}
		}
		return this;
	}
	
	public InfluxDBQuery gt(String fieldName,Object val) {
		if (!"where".equals(querySql.substring((querySql.length() - 6), querySql.length()).trim())) {
			querySql += " and ";
		}
		if(fieldName.indexOf(timeField) > -1) {
			querySql += fieldName +" > '"+val+"'" ;
		}
		else {
			if(val instanceof String) {
				querySql += fieldName +" > '"+val+"'" ;
			}
			else {
				querySql += fieldName +" > "+val ;
			}
		}
		return this;
	}
	
	public InfluxDBQuery gte(String fieldName,Object val) {
		if (!"where".equals(querySql.substring((querySql.length() - 6), querySql.length()).trim())) {
			querySql += " and ";
		}
		if(fieldName.indexOf(timeField) > -1) {
			querySql += fieldName +" >= '"+val+"'" ;
		}
		else {
			if(val instanceof String) {
				querySql += fieldName +" >= '"+val+"'" ;
			}
			else {
				querySql += fieldName +" >= "+val ;
			}
		}
		return this;
	}
	
	public InfluxDBQuery lt(String fieldName,Object val) {
		if (!"where".equals(querySql.substring((querySql.length() - 6), querySql.length()).trim())) {
			querySql += " and ";
		}
		if(fieldName.indexOf(timeField) > -1) {
			querySql += fieldName +" < '"+val+"'" ;
		}
		else {
			if(val instanceof String) {
				querySql += fieldName +" < '"+val+"'" ;
			}
			else {
				querySql += fieldName +" < "+val ;
			}
		}
		return this;
	}
	
	public InfluxDBQuery lte(String fieldName,Object val) {
		if (!"where".equals(querySql.substring((querySql.length() - 6), querySql.length()).trim())) {
			querySql += " and ";
		}
		if(fieldName.indexOf(timeField) > -1) {
			querySql += fieldName +" <= '"+val+"'" ;
		}
		else {
			if(val instanceof String) {
				querySql += fieldName +" <= '"+val+"'" ;
			}
			else {
				querySql += fieldName +" <= "+val ;
			}
		}
		return this;
	}
	
	public String bulid() {
		if(querySql.indexOf("select") != 0) {
			querySql = "select *"+querySql;
		}
		if (!"where".equals(querySql.substring((querySql.length() - 6), querySql.length()).trim())) {
			querySql += " and enabled = '1' ";
		}
		else {
			querySql += " enabled = '1' ";
		}
		querySql += timeZone+";";
		return this.querySql;
	}
	
	public String getSql() {
		String sql = this.querySql;
		if(querySql.indexOf("select") != 0) {
			sql = "select *"+querySql;
		}
		if (!"where".equals(querySql.substring((querySql.length() - 6), querySql.length()).trim())) {
			sql += " and enabled = '1' ";
		}
		else {
			sql += " enabled = '1' ";
		}
		return sql;
	}
}
