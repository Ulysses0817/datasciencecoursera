import random, pymysql

class ReportGenerator():
    '''
    '''
    def __init__(self):
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
        alarm_col = ["Date", "ind_1_alarm", "ind_2_alarm", "ind_3_alarm", "ind_4_alarm", "ind_5_alarm", "ind_6_alarm"]
        resu_sql = "select %s from %s where Date=(select max(Date) from %s)"%(",".join(alarm_col), mpid_info[0], mpid_info[0])
        self._cursor.execute(resu_sql)
        alarm_data = self._cursor.fetchone()
        #print(alarm_data)
        alarm_value = max(alarm_data[1:])
        return str(alarm_value), alarm_data[0]
        
    def give_suggestion(self, alarm_value, mp=True):
        '''
        '''
        print(alarm_value=="0")
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
        #print(currentstate)
        return currentstate, suggestion
        
    def combiner(self, alarm_date, mpid_info, alarm_value, mp_currentstate, mp_suggestion, equip_currentstate, equip_suggestion):
        '''
        '''
        #mp_currentstate = {"0":"正常", "1":"预警", "2":"报警", "3":"停机"}
        if alarm_value != "0":
            mp_result = str(alarm_date) + mpid_info[-2] + mpid_info[-3] + mp_currentstate + "," + mp_suggestion
        else:
            mp_result = mpid_info[-2] + "的" + mpid_info[-3] + mp_currentstate + "," + mp_suggestion
            
        equip_result = mpid_info[-2] + equip_currentstate + "," + equip_suggestion
            
        insert_d = tuple([alarm_date] + list(mpid_info[1:]) + [mp_currentstate, mp_suggestion, mp_result, equip_currentstate, equip_suggestion, equip_result])
        #print(insert_d)
        insert_sql = '''insert into report_result(fault_time, mp_id, equip_id, line_id, mp_name, equip_name, corresp_parts, mp_currentstate,\
                    mp_suggestions,mp_result,equip_currentstate, equip_suggestions, equip_result)\
                    values (str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%S'), \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\')'''%(insert_d)
        print(insert_sql)
        return insert_sql
    
    def run(self, column = ["cm_results_table", "mp_id", "equip_id", "line_id", "mp_name", "equip_name", "corresp_parts"], 
                      table_name = "cd_monitoring_task", equip_id = "EQPID001"):
        '''
        '''
        mpids_info = self.get_mpid_info(column, table_name, equip_id)
        alarm_value_list = []
        mp_suggestions = []
        mp_currentstates = []
        alarm_dates = []
        mp_results = []
        for i in mpids_info:
            alarm_value, alarm_date = self.get_latest_alarm(i)
            alarm_value_list.append(alarm_value)
            alarm_dates.append(alarm_date)
            
            mp_currentstate, mp_suggestion = self.give_suggestion(alarm_value)
            mp_suggestions.append(mp_suggestion)
            mp_currentstates.append(mp_currentstate)
        #print(alarm_dates)
        equip_currentstate, equip_suggestion = self.give_suggestion(max(alarm_value_list), mp=False)
        for i in range(len(alarm_value_list)):
            insert_sql = self.combiner(alarm_dates[i], mpids_info[i], alarm_value_list[i], mp_currentstates[i],
                                       mp_suggestions[i], equip_currentstate, equip_suggestion)
            self._cursor.execute(insert_sql)
            
        self.connection.commit()
        #self.connection_close()
        
            
            
            
            