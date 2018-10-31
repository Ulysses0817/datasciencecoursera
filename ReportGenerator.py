import random, pymysql, os, json
from utils.mysql_cmd import MYSQL

class ReportGenerator():
    '''
    '''
    def __init__(self, host, port, user, password, db, charset, equip_id):
    
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset
        self.equip_id = equip_id
        self._corpus =  { "0" : "请进行常规维护",
                          "1" : (["萌生初期故障",
                                 "存在早期故障",
                                 "具有早期微弱故障征兆"
                                ],
                                ["建议用户密切关注该测点的健康指标走势，及时掌握故障程度的发展", 
                                 "建议用户着重留意该测点附近的零部件，必要时相应的信号进行分析，排除故障风险",
                                 "建议用户重点关注该测点附近零部件的故障发展趋势，并定时排查",
                                 "建议用户密切关注该测点的信号变化，通过指标变化关注早期故障的发展"
                                ],
                                ["建议用户留意预警测点的健康指标趋势，做好故障预防措施"
                                ]),

                          "2" : (["存在故障",
                                  "已经发生故障",
                                  "很大程度上出现故障",
                                  "出现故障的几率较大",
                                 ],
                                 ["建议用户停机检查或者更换零部件",
                                  "建议用户准备好备用零部件，对该测点附件零部件进行维修",
                                  "故障有进一步扩展的趋势，建议用户尽快更换零部件",
                                  "建议用户定时进行现场勘查，密切关注设备运行状态，及时维修异常零部件",
                                ],
                                ["建议用户重点关注，最好停机检查进行维修"
                                ])          
                          }
		self.__data = {}
		
    def run(self):
        
        self._dbconnection()
        
        if self.equip_id == 'all':
            
        else:
            mpid_info = self.get_mpid_info(self.equip_id)
	
    def _dbconnection(self):
        '''
        函数功能：配置连接数据库
        '''
        self.my_conn = MYSQL(host = self.host, port = self.port, user = self.user,
                             password = self.password, db = self.db, charset = self.charset)
        
    def get_mpid_info(self, equip):
        
        sql = "select mp_id, mp_name, equip_id, line_id, mp_name, equip_name, corresp_parts from cd_monitoring_task where equip_id = '%s'" % (equip)
        
        try:
            iterm_tuple = self.my_conn.inquire_all(sql)   
        except Exception as e:  
            print("出现问题1：" + str(e))
        for iterm_list in iterm_tuple:
			mpid_info = {'mp_id': iterm_list[0], 'mp_name': iterm_list[1], 'equip_id': iterm_list[2], 'line_id': iterm_list[3], 'mp_name': iterm_list[4], 
			             'equip_name': iterm_list[5], 'corresp_parts': iterm_list[6]}
        self.__data[equip][iterm_list[0]] = mpid_info
		
        return mpid_info
		
    def get_mpid_info(self, column = ["cm_results_table", "mp_id", "equip_id", "line_id", "mp_name", "equip_name", "corresp_parts"], 
                      table_name = "cd_monitoring_task", equip_id = "EQPID001"):
        '''
        '''
        if equip_id:
            mpid_sql = "select %s from %s where equip_id = '%s'"%(",".join(column), table_name, equip_id)
        else:
            mpid_sql = "select %s from %s "%(",".join(column), table_name)
        print(mpid_sql)
        
        self._cursor.execute(mpid_sql)
        mpids_info = self._cursor.fetchall()
        
        return mpids_info
    
    def get_latest_alarm(self, mpid_info):
        '''
        '''
        alarm_col = ["create_time", "alarm"]
        resu_sql = "select %s from %s where mp_id = \'%s\'"%(",".join(alarm_col), "cm_result", mpid_info[1])
        self._cursor.execute(resu_sql)
        alarm_data = self._cursor.fetchall()
        alarm_date = "0000"
        for i in alarm_data:
            if str(i[0]) > alarm_date:
                alarm_date = str(i[0])
                alarm_value = str(int(i[1]))
        return alarm_value, alarm_date
        
    def give_suggestion(self, alarm_value, mp=True):
        '''
        '''
        print(alarm_value, alarm_value=="0")
        if mp:
            # 判断测点状态编号
            if alarm_value == "0":
                suggestion = self._corpus["0"]
                currentstate = "正常"
                #print(suggestion)
            elif alarm_value == "1":
                condition = self._corpus["1"]
                suggestion = ",".join([random.choice(condition[0]), random.choice(condition[1])])
                currentstate = "预警"
                #print(suggestion)
            elif alarm_value == "2":
                condition = self._corpus["2"]
                suggestion = ",".join([random.choice(condition[0]), random.choice(condition[1])])
                currentstate = "报警"
                #print(suggestion)
            elif alarm_value == "3":
                suggestion = "None"
                currentstate = "停机"
        else:
            # 判断设备状态编号
            if alarm_value == "0":
                suggestion = self._corpus["0"]
                currentstate = "正常"
                #print(suggestion)
            elif alarm_value == "1":
                condition = self._corpus["1"]
                suggestion = random.choice(condition[2])
                currentstate = "预警"
                #print(suggestion)
            elif alarm_value == "2":
                condition = self._corpus["2"]
                suggestion = random.choice(condition[2])
                currentstate = "报警"
                #print(suggestion) 
            elif alarm_value == "3":
                suggestion = "None"
                currentstate = "停机"
        #print(currentstate)
        return currentstate, suggestion
        
    def combiner(self, alarm_date, mpid_info, alarm_value, mp_currentstate, mp_suggestion, equip_currentstate, equip_suggestion):
        '''
        '''
        #mp_currentstate = {"0":"正常", "1":"预警", "2":"报警", "3":"停机"}
        if alarm_value in ["1", "2"] and equip_currentstate != "停机":
            mp_result = str(alarm_date) + mpid_info[-2] + mpid_info[-3] + mp_currentstate + "," + mp_suggestion
            equip_result = mpid_info[-2] + equip_currentstate + "," + equip_suggestion
        elif alarm_value == "0" and equip_currentstate != "停机":
            mp_result = mpid_info[-2] + "的" + mpid_info[-3] + mp_currentstate + "," + mp_suggestion
            equip_result = mpid_info[-2] + equip_currentstate + "," + equip_suggestion
        else:#elif alarm_value == "3":
            mp_result = "None"
            mp_suggestion = "None"
            equip_result = "None"
            
        insert_d = tuple([alarm_date] + list(mpid_info[1:]) + [mp_currentstate, mp_suggestion, mp_result, equip_currentstate, equip_suggestion, equip_result])
        #print(insert_d)
        insert_sql = '''insert into report_result(fault_time, mp_id, equip_id, line_id, mp_name, equip_name, corresp_parts, mp_currentstate,\
                    mp_suggestions,mp_result,equip_currentstate, equip_suggestions, equip_result)\
                    values (str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%S'), \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\')'''
        
        print(insert_sql%(insert_d))
        return insert_sql%(insert_d)
    
    def run(self, column = ["cm_results_table", "mp_id", "equip_id", "line_id", "mp_name", "equip_name", "corresp_parts"], 
                      table_name = "cd_monitoring_task", equip_id = "EQPID001"):
        '''
        '''
        self.data = {}
        mpids_info = self.get_mpid_info(column, table_name, equip_id)
        for i in mpids_info:
            self.data[i[2]] = {}        
            self.data[i[2]]["alarm_value_list"] = []
            self.data[i[2]]["mp_suggestions"] = []
            self.data[i[2]]["mp_currentstates"] = []
            self.data[i[2]]["alarm_dates"] = []
            self.data[i[2]]["mpids_info"] = []
        #equip_currentstates = []
        #equip_suggestions = [
        for i in mpids_info:
            self.data[i[2]]["mpids_info"].append(i)
            alarm_value, alarm_date = self.get_latest_alarm(i)
            self.data[i[2]]["alarm_value_list"].append(alarm_value)
            self.data[i[2]]["alarm_dates"].append(alarm_date)
            
            mp_currentstate, mp_suggestion = self.give_suggestion(alarm_value)
            self.data[i[2]]["mp_suggestions"].append(mp_suggestion)
            self.data[i[2]]["mp_currentstates"].append(mp_currentstate)
        #print(alarm_dates)
        for i in self.data:
            self.data[i]["equip_currentstate"], self.data[i]["equip_suggestion"] = self.give_suggestion(max(self.data[i]["alarm_value_list"]), mp=False)
        self._cursor.execute("TRUNCATE TABLE report_result")
        for k in self.data:
            datum = self.data[k]
            for i in range(len(datum["alarm_value_list"])):
                insert_sql = self.combiner(datum["alarm_dates"][i], datum["mpids_info"][i], datum["alarm_value_list"][i], 
                datum["mp_currentstates"][i], datum["mp_suggestions"][i], datum["equip_currentstate"], datum["equip_suggestion"])
                self._cursor.execute(insert_sql)
            
        self.connection.commit()
        #self.connection_close()
        
            
            
            
            
