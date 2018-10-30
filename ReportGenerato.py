import random, pymysql, os, time
import json
import codecs

class ReportGenerator():
    '''
    '''
    def __init__(self):
        self.data = {}
        self.old_data = {}
        self.connection = None
        self._cursor = None
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
    
    def _connection(self, host="127.0.0.1", port=3306, user='root', password='li0123', db='db', charset='utf8'):
        '''
        '''
        self.connection = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset=charset)
        self._cursor = self.connection.cursor()
    
    def connection_close(self):
        self._cursor.close()
        self.connection.close()
    
    def get_old_data(self):
        '''
        '''
        self._cursor.execute("select * from report_result")
        
    def get_mpid_info(self, column = ["cm_results_table", "mp_id", "equip_id", "line_id", "mp_name", "equip_name", "corresp_parts"], 
                      table_name = "cd_monitoring_task", equip_id = "all"):
        '''
        '''
        if equip_id and "EQPID" in equip_id:
            mpid_sql = "select %s from %s where equip_id = '%s'"%(",".join(column), table_name, equip_id)
        else :
            mpid_sql = "select %s from %s "%(",".join(column), table_name)
        #print(mpid_sql)
        
        self._cursor.execute(mpid_sql)
        mpids_info = self._cursor.fetchall()
        
        return mpids_info
    
    def get_latest_alarm(self, mpid_info):
        '''
        '''
        alarm_col = ["create_time", "alarm"]
        resu_sql = "select %s from %s where mp_id = \'%s\'"%(",".join(alarm_col), "cm_result", mpid_info[1])
        # print(resu_sql)
        self._cursor.execute(resu_sql)
        alarm_data = self._cursor.fetchall()
        if not alarm_data:
            raise TypeError("didn't fetch the correspanding mpid!")
        # print(alarm_data)

        alarm_DATA = []
        for i in alarm_data:
            alarm_DATA.append((time.mktime(i[0].timetuple()), str(int(i[1]))))
        alarm_DATA.sort(key=lambda k:k[0])
                
        alarm_date = str(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(alarm_DATA[-1][0])))
        alarm_value = alarm_DATA[-1][1]
		
        # alarm_date = "0000"
        # for i in alarm_data:
            # if str(i[0]) > alarm_date:
                # alarm_date = str(i[0])
                # alarm_value = str(int(i[1]))
        # print(alarm_date)
        return alarm_value, alarm_date
        
    def give_suggestion(self, alarm_value, mp=True):
        '''
        '''
        #print(alarm_value, alarm_value=="0")
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
        
        # print(insert_sql%(insert_d))
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
            self.data[i[2]]["pics_path"] = []
            self.data[i[2]]["log_judge"] = []
        #equip_currentstates = []
        #equip_suggestions = []
        
        for i in mpids_info:
            self.data[i[2]]["mpids_info"].append(i)
            alarm_value, alarm_date = self.get_latest_alarm(i)
            self.data[i[2]]["alarm_value_list"].append(alarm_value)
            self.data[i[2]]["alarm_dates"].append(alarm_date)
            
            self.data[i[2]]["log_judge"].append(tuple([i[1], alarm_value]))
            
            mp_currentstate, mp_suggestion = self.give_suggestion(alarm_value)
            self.data[i[2]]["mp_suggestions"].append(mp_suggestion)
            self.data[i[2]]["mp_currentstates"].append(mp_currentstate)
        #print(alarm_dates)
		
        for i in self.data:
            self.data[i]["equip_currentstate"], self.data[i]["equip_suggestion"] = self.give_suggestion(max(self.data[i]["alarm_value_list"]), mp=False)
            
        # temp_dict = {}
        # for k, v in self.data
            # temp_dict["alarm_value_list"] = self.data["alarm_value_list"]
        #print(os.getcwd())
        # print(os.path.realpath(__file__))
        self._cursor.execute("TRUNCATE TABLE report_result")
        
        log_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log.json')
        if not os.path.exists(log_path):
            # 如果没有日志文件进行对比，直接写入新的报告
            for k in self.data:
                datum = self.data[k]
                for i in range(len(datum["alarm_value_list"])):
                    insert_sql = self.combiner(datum["alarm_dates"][i], datum["mpids_info"][i], datum["alarm_value_list"][i],
                    datum["mp_currentstates"][i], datum["mp_suggestions"][i], datum["equip_currentstate"], datum["equip_suggestion"])
                    self._cursor.execute(insert_sql)
        else:
            # 读取日志文件
            with open(log_path) as f:
                old_data = json.load(f)
            
            for k in self.data:
                datum = self.data[k]
                olddatum = old_data.get(k, 0)
                # 如果该设备id日志文件中没有，或者最大警戒值不同，写入新的报告
                if olddatum == 0 or max(datum["alarm_value_list"]) != max(olddatum["alarm_value_list"]):
                    for i in range(len(datum["alarm_value_list"])):
                        insert_sql = self.combiner(datum["alarm_dates"][i], datum["mpids_info"][i], datum["alarm_value_list"][i], 
                        datum["mp_currentstates"][i], datum["mp_suggestions"][i], datum["equip_currentstate"], datum["equip_suggestion"])
                        self._cursor.execute(insert_sql)
                # 如果最大警戒值相同，结合日志信息写入报告
                elif max(datum["alarm_value_list"]) == max(olddatum["alarm_value_list"]):
                    for i in range(len(datum["alarm_value_list"])):
                        if datum["log_judge"][i] in olddatum["log_judge"]:
                            temp_i = olddatum["log_judge"].index(datum["log_judge"][i])
                            insert_sql = self.combiner(datum["alarm_dates"][i], datum["mpids_info"][i], datum["alarm_value_list"][i], 
                            olddatum["mp_currentstates"][itemp_i], olddatum["mp_suggestions"][itemp_i], olddatum["equip_currentstate"], 
                            olddatum["equip_suggestion"])
                            self._cursor.execute(insert_sql)
                        else:
                            insert_sql = self.combiner(datum["alarm_dates"][i], datum["mpids_info"][i], datum["alarm_value_list"][i],
                            datum["mp_currentstates"][i], datum["mp_suggestions"][i], datum["equip_currentstate"], datum["equip_suggestion"])
                            self._cursor.execute(insert_sql)
                    
        self.connection.commit()
        
        with open(log_path, 'w') as outfile:
            json.dump(self.data, outfile)
                
        
        #self.connection_close()
        
            
            
            
            