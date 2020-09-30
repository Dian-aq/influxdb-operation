package top.dianay.influxdb.utils;

import com.google.gson.Gson;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import top.dianay.influxdb.InfluxDBHolder;
import top.dianay.influxdb.InfluxdbProperties;

import java.text.ParseException;
import java.util.*;

public class InfluxDBOperatingUtils {

	private static Logger log = LoggerFactory.getLogger(InfluxDBOperatingUtils.class);

	/**[0]measurePointId,[1]enabled*/
	public static final String[] tagFieldNames = new String[] {"measurePointId","enabled"};

	public static final String timeField = "time";

	private static Gson gson = new Gson();

	public static void insertData(String measurement,Map<String,String> tagMap,Map<String,Object> insertMap) {
		if(insertInComeCheck(measurement,tagMap,insertMap)) {
			Builder pointBuild = Point.measurement(measurement);
			Object timeStr = MapUtils.getObject(insertMap, timeField, null);
			if(timeStr != null) {
				Long timel = handleInsertTime(timeStr);
				if(timel != null) {
					insertMap.remove(timeField);
					if(insertMap.size() > 0) {
						pointBuild.time(timel, InfluxdbProperties.timeUnit);
						pointBuild.fields(insertMap);
						pointBuild.tag(tagMap);
						try {
							InfluxDBHolder.getConnect().write(pointBuild.build());
						}
						catch (Exception e) {
							log.error("insert influxDB error!"+gson.toJson(pointBuild));
							e.printStackTrace();
						}
					}
				}
			}
			else {
				log.error(" wirte error ! miss pointBuild!");
			}
		}
	}
	
	public static List<Map<String,Object>> findData(String searchSql){
		Query query = new Query(searchSql, InfluxDBHolder.dataBase);
		QueryResult qresult = InfluxDBHolder.getConnect().query(query);
		return handleSearchResultData(qresult,null);
	}
	
	public static List<Map<String,Object>> findDataAndHandleTime(String searchSql,String dateFormula){
        Query query = new Query(searchSql, InfluxDBHolder.dataBase);
        QueryResult qresult = InfluxDBHolder.getConnect().query(query);
        return handleSearchResultData(qresult,dateFormula);
    }
	
	public static List<Map<String,Object>> findData(String measurement,List<String> searchFields,Map<String,Object> condition){
		return baseFindHandle(measurement,null,searchFields, condition,null);
	}
	
	public static List<Map<String,Object>> findData(String measurement,List<String> searchFields,Map<String,Object> condition,List<String> groupFieldList){
		return baseFindHandle(measurement,null,searchFields, condition,groupFieldList);
	}
	
	public static List<Map<String,Object>> findData(String measurement,String policies,List<String> searchFields,Map<String,Object> condition){
		return baseFindHandle(measurement,policies, searchFields, condition,null);
	}
	
	/**
	 * 查询数据
	 * @return
	 */
	private static List<Map<String,Object>> baseFindHandle(String measurement,String policies,List<String> searchFields,Map<String,Object> condition,List<String> groupFieldList){
		StringBuilder queryCommSb = new StringBuilder();
		queryCommSb.append("select ");
		//查询字段
		if(CollectionUtils.isEmpty(searchFields)) {
			queryCommSb.append(" * ");
		}
		else {
			int filedIndex = searchFields.size() - 1;
			for(int i = 0;i <= filedIndex;i++) {
				if(i == filedIndex) {
					queryCommSb.append(searchFields.get(i));
				}
				else {
					queryCommSb.append(searchFields.get(i));
					queryCommSb.append(",");
				}
			}
		}
		queryCommSb.append(" from ");
		if(StringUtils.isNotEmpty(policies)) {
			queryCommSb.append(InfluxDBHolder.dataBase+"."+policies+"."+measurement);
		}
		else {
			queryCommSb.append(measurement);
		}
		boolean mustAddTimeZone = false;
		//条件拼接
		if(condition != null && condition.size() > 0) {
			queryCommSb.append(" where ");
			int conditionNum = condition.size();
			for(Map.Entry<String, Object> conditionEntry : condition.entrySet()) {
				conditionNum--;
				String fieldName = conditionEntry.getKey();
				Object val = conditionEntry.getValue();
				if(fieldName.indexOf(timeField) > -1) {
					queryCommSb.append(fieldName +"'"+val+"'" );
					mustAddTimeZone = true;
				}
				else {
					if(val instanceof String) {
						queryCommSb.append(fieldName +"'"+val+"'" );
					}
					else {
						queryCommSb.append(fieldName + val );
					}
				}
				if(conditionNum > 0) {
					queryCommSb.append(" and ");
				}
			}
		}
		//group by
		if(CollectionUtils.isNotEmpty(groupFieldList)) {
			queryCommSb.append(" group by ");
			int groupFieldIndex = groupFieldList.size() - 1;
			for(int i = 0;i <= groupFieldIndex;i++) {
				if(i == groupFieldIndex) {
					queryCommSb.append(groupFieldList.get(i));
				}
				else {
					queryCommSb.append(groupFieldList.get(i));
					queryCommSb.append(",");
				}
			}
		}
		if(mustAddTimeZone) {
			queryCommSb.append(" tz('Asia/Shanghai') ;");
		}
		Query query = new Query(queryCommSb.toString(), InfluxDBHolder.dataBase);
		QueryResult qresult = InfluxDBHolder.getConnect().query(query);
		return handleSearchResultData(qresult,null);
	}
	

	private static List<Map<String,Object>> handleSearchResultData(QueryResult qresult,String dateFormula){
		List<Map<String,Object>> reusltListMap = new ArrayList<>();
		List<Result> resultList = qresult.getResults();
		if(CollectionUtils.isNotEmpty(resultList)) {
			for(Result rs : resultList) {
				List<Series> srList = rs.getSeries();
				if(CollectionUtils.isNotEmpty(srList)) {
					for(Series sr : srList) {
						List<String> colList = sr.getColumns();
						Map<String,String> tagsMap = sr.getTags();
						List<List<Object>> valList = sr.getValues();
						if(CollectionUtils.isNotEmpty(valList)) {
							for(List<Object> dataList : valList) {
								if(CollectionUtils.isNotEmpty(dataList)) {
									Map<String,Object> dataMap = new HashMap<String, Object>();
									for(int i = 0;i < dataList.size();i++) {
										Object val = dataList.get(i);
										if(val instanceof Number) {
											double doubleVal = (double) val;
											float floatVal = (float) doubleVal;
											dataMap.put(colList.get(i),floatVal);
										}
										else {
											//日期字段处理
											if(timeField.equalsIgnoreCase(colList.get(i))) {
											    if(StringUtils.isEmpty(dateFormula)) {
											        Date date = findResultTimeFieldHandle(val);
	                                                dataMap.put(colList.get(i),date);
											    }
											    else {
											        Date date = findResultTimeFieldHandle(val);
											        String dateStr = InfluxDBDateUtil.format(date,dateFormula);
                                                    dataMap.put(colList.get(i),dateStr);
											    }
											}
											else {
												dataMap.put(colList.get(i),val);
											}
										}
									}
									if(tagsMap != null) {
										dataMap.putAll(tagsMap);
									}
									reusltListMap.add(dataMap);
								}
							}
						}
					}
				}
			}
		}
		return reusltListMap;
	}
	

	private static Date findResultTimeFieldHandle(Object timeStr) {
		Date handleResult = null;
		try {
			handleResult = InfluxDBDateUtil.parse((String)timeStr, "yyyy-MM-dd'T'HH:mm:ss'+08:00'");
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return handleResult;
	}
	

	private static Long handleInsertTime(Object time) {
		Long timeLong = null;
		if(time instanceof Date) {
			timeLong = ((Date) time).getTime() / 1000;
		}
		else if(time instanceof String) {
			String timeStr = (String)time;
			try {
				Date date = DateUtils.parseDate(timeStr,"yyyy-MM-dd HH:mm:ss");
				timeLong = date.getTime() / 1000;
			}
			catch (Exception e) {
				log.error(time+" date format error!");
				timeLong = null;
			}
		}
		else {
			timeLong = null;
		}
		return timeLong;
	}
	

	private static boolean insertInComeCheck(String measurement,Map<String,String> tagMap,Map<String,Object> insertMap) {
		boolean checkResult = true;
		if(StringUtils.isEmpty(measurement)) {
			checkResult = false;
			log.error("insert measurement is null!");
		}
		if(insertMap == null || insertMap.size() == 0) {
			checkResult = false;
			log.error("insert dataMap is null!");
		}
		for(String tagFieldName : tagFieldNames) {
			if(!tagMap.containsKey(tagFieldName)) {
				checkResult = false;
				log.error("insert dataMap is not existence tag field!");
			}
		}
		return checkResult;
	}
	
	private InfluxDBOperatingUtils() {
		throw new IllegalStateException("Utility class");
	}
}
