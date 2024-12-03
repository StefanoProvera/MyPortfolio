# Mobility as a Service (MaaS) Simulation and Optimization Code
# ---------------------------------------------------------------
# Developed to simulate and optimize a self-driving car-sharing service in the city of Brescia.
# The project analyzes operational and economic feasibility based on real-world trip data.
# ---------------------------------------------------------------

# Import necessary libraries
import pandas as pd
import numpy as np
import os
import bisect
import time
from tqdm import tqdm
import multiprocessing as mp
import datetime
from math import radians, cos, sin, asin, sqrt, exp
import scipy.optimize as sc_opt


# ---------------------------------------------------------------
# Class: Simulator
# ---------------------------------------------------------------
# Purpose: Simulate the autonomous and electric car-sharing service.
# It includes functionality for initializing vehicles, managing charging stations,
# and processing user trip requests.

class Simulator:
    ### definition simulator of autonomous and electric sharing service
    def __init__(self, path, range_to_test, radius, n_charging_stations):
        # Simulation data import
        self.path = path  ### path of the trip dataset
        self.df = pd.read_parquet(self.path) ### trip dataset to simulate
        # the service
        self.df.time_start = pd.to_datetime(self.df.time_start)
        self.df.time_end = pd.to_datetime(self.df.time_end)
        self.df = self.df.sort_values(by='time_start') ## sort by
        # time_start = time of the calls in the service
        self.df = self.df.reset_index()
        self.df.distance = self.df.distance / 1000 ## from meter
        # to kilometer
        self.df_list = []

        #read df containg the distribution of trips' starts,
        # used in order to place recharging stations
        self.zones_trips = pd.read_parquet(
            r"...\Distribuzione_partenze15.0.parquet",
            engine='fastparquet')

        ### order zones by the number of trips' starts
        self.zones_trips.sort_values('Starting_point',
                                     ascending=False,inplace=True)

        self.maximum_number_stations = 20  # keep divisible by 4
        # (or change code in InitializeRechargingStations)
        self.InitializeRechargingStations(radius,
                      n_charging_stations) # Place charging stations
        self.every_station_occupied = False

        # final_df contains information about the vehicles in the service
        self.final_df=pd.DataFrame(columns=['index','latitude', 'longitude',
                      'busy_until', 'distance', 'overhead',
                      'dead_time', 'tot_calls', 'time_waiting_recharge',
                                        'time_recharging','tot_recharges'])


        self.EARLY = -1 ## used in order to setup an early stop
        # in the simulation process

        self.RANGE_TO_TEST = range_to_test  #number of vehicles in
        # the service
        # lists containing the vehicles in the service
        self.free_cars = []
        self.busy_cars = []
        self.free_cars_recharging = []
        # Set speed for an autonomous vehicle
        self.L3VehicleSpeed = 15 # km/h

        self.N_PARALLEL = 1 #one simulation at time per optimization

        # Max time the user is willing to wait for the car to arrive.
        self.USER_WAIT_FOR_CAR = 20 # [min]

        # Parameters Car
        self.battery_capacity = 60 #in kwh
        self.efficiency_battery = 0.975
        self.consumption = 0.2 # in kwh/km
        self.recharging_power = 13 #in kw


        ### thresholds used in the charging policy of the service
        self.threshold_send_recharge_day = 25
        self.threshold_car_free_day = 30
        self.threshold_send_recharge_night = 60
        self.threshold_car_free_night = 90
        ## initialize thresholds and nighttime
        self.threshold_send_recharge = self.threshold_send_recharge_day
        self.threshold_car_free = self.threshold_car_free_day
        self.night = False



        # List of all the user wait time
        self.user_wait_time = []
        # List of the number of free vehicles at each iteration
        self.free_cars_v = []
        # List of all the skipped calls
        self.skipped_calls = []

    def InitializeRechargingStations(self, radius, N_charging_stations):
        ###
        # This function takes as input the radius of the coverage area
        # and the total number of the charging stations to place and
        # create a dataframe with the initialized stations
        ###

        #select just subregions inside the coverage area
        lat_centre_radius = 45.53936
        #taken from charging_places, always fixed
        lon_centre_radius = 10.22218 # coordinates of Brescia city centre
        rows_to_drop = []
        for index, row in self.zones_trips.iterrows():
            distance = haversine(lon_centre_radius,lat_centre_radius,
             (row['West']+row['East'])/2,(row['North']+row['South'])/2)
            if distance>radius:
                rows_to_drop.append(index)

        self.zones_trips.drop(rows_to_drop,inplace=True)

        # find maximum number of starting points
        max_starting_points = self.zones_trips['Starting_point'].max()
        counter_placed_stations = 0
        parameter_division = 1


        #now the number of charging station in the selected position
        # is proportional to the number of trips starting from there

        self.charging_stations = pd.DataFrame(columns=['latitude',
               'longitude','available_stations','occupied_stations'])
        for index,row in self.zones_trips.iterrows():
            if row['Starting_point']<max_starting_points/1.5 and \
                    row['Starting_point']>=max_starting_points/2:
                parameter_division = 4/3
            if row['Starting_point']<max_starting_points/2 and \
                    row['Starting_point']>=max_starting_points/4:
                parameter_division = 2
            if row['Starting_point']<max_starting_points/4:
                parameter_division = 4
            if counter_placed_stations < N_charging_stations:
                self.charging_stations.loc[len(self.charging_stations)] = \
                [(row['North']+row['South'])/2,(row['West']+row['East'])/2,
                self.maximum_number_stations/parameter_division,0]
                counter_placed_stations += \
                    self.maximum_number_stations/parameter_division
                if counter_placed_stations > N_charging_stations:
                    self.charging_stations.loc[len
                                   (self.charging_stations)-1] = \
                        [(row['North'] + row['South']) / 2,
                         (row['West'] + row['East']) / 2,
                        self.maximum_number_stations / parameter_division
                         - counter_placed_stations + N_charging_stations, 0]
            else:
                break





    def FreeChargingStation(self, used_car,**kwargs):
        ###this function is used when a car leaves the charging station

        time_req = kwargs.get('time_req', None)
        ## find the charging zone where the car was recharging and free it
        latitude=used_car[0]
        longitude=used_car[1]
        index_selected_station = self.charging_stations.index[
            (self.charging_stations['latitude']==latitude) &
            (self.charging_stations['longitude']==longitude)]
        self.charging_stations.loc[index_selected_station,
                                   'available_stations'] += 1
        self.charging_stations.loc[index_selected_station,
                                   'occupied_stations'] -= 1
        self.every_station_occupied = False
        if time_req!=None:
            #now update State of Charge until the last moment
            time_delta = (time_req - used_car[12]).total_seconds()
            used_car[7] = self.newSOC(used_car[7], True,
                                      delta_time=time_delta)
            # update tot_time recharging and other parameters
            # of the vehicle
            used_car[10] = used_car[10] + time_delta / 3600
            used_car[12] = time_req
            used_car[13].append(used_car[7])
            used_car[14].append(used_car[12])
        #car no longer recharging
        used_car[8] = False

        return used_car

    def SendToChargingStation(self, chosen_vehicle,actual_time):
        ### when a vehicle needs to recharge, this function finds
        # the closest available station and send the car there

        #find the closest charging station available
        lats = self.charging_stations.loc[self.charging_stations
                      ['available_stations']>0,'latitude'].tolist()
        lons = self.charging_stations.loc[self.charging_stations
                      ['available_stations']>0,'longitude'].tolist()

        if len(lats) == 0:
            self.every_station_occupied=True
            return [], False  ## not currently available stations

        vehicle_pos = [chosen_vehicle[0],chosen_vehicle[1]]
        distances = 1.5 * haversine_distance(lats, lons,
             vehicle_pos) #find distances of the stations from the car

        # Choose the closest station
        index_min = min(range(len(distances)), key=distances.__getitem__)
        index_selected_station = \
            self.charging_stations.index[(self.charging_stations['latitude']
                                          ==lats[index_min]) &
                                         (self.charging_stations['longitude']==
                                          lons[index_min])]
        self.charging_stations.loc[index_selected_station,'available_stations'] -= 1
        self.charging_stations.loc[index_selected_station, 'occupied_stations'] += 1

        # now update all data of the current vehicle
        distance_to_station = distances[index_min]
        time_to_station = 3600*distance_to_station/self.L3VehicleSpeed #this
        # is a time in seconds

        chosen_vehicle[7]=self.newSOC(chosen_vehicle[7], charging=False,
                            tot_distance=distance_to_station)
        newtime=actual_time+pd.to_timedelta((time_to_station), unit='s')

        chosen_vehicle[13].append(chosen_vehicle[7])
        chosen_vehicle[14].append(newtime)
        chosen_vehicle[0] = lats[index_min]
        chosen_vehicle[1] = lons[index_min]
        chosen_vehicle[3] = chosen_vehicle[3]+distance_to_station
        chosen_vehicle[4] = chosen_vehicle[4]+distance_to_station
        chosen_vehicle[8] = True
        chosen_vehicle[11] = chosen_vehicle[11]+1
        chosen_vehicle[12] = newtime
        chosen_vehicle[15] = newtime+datetime.timedelta(seconds=900)

        return chosen_vehicle, True


    def InitializeCars(self, N_sharcars):

        ## this function initializes all the vehicles in the service
        ## here below all the tracked statistics during the simulation process

        # 0,        1,         2,          3,        4,        5,         6,
        # Latitude, longitude, busy_until, distance, overhead, dead_time, tot_calls,
        # 7,                8,               9,                          10,              11,
        # State of Charge,  Charging Status, time_waiting_free_stations, time_recharging, tot_charges,
        # 12,                  13,            14,                15,               16,
        # last_check_recharge, SOC evolution ,time_soc_change,  new_need_check  ,#id_car

        ########### use list  as containers of cars
        self.free_cars=[]
        self.busy_cars=[]
        self.free_cars_recharging = []
        #initialize all the cars
        for i in range(N_sharcars):
            self.busy_cars.append([self.df.iloc[i].latitude_start,
                                       self.df.iloc[i].longitude_start,
                                       self.df.iloc[i].time_start, 0, 0, 0, 0, 100, False,
                                       0, 0, 0,self.df.iloc[0].time_start,[100],[self.df.iloc[0].time_start],
                                       self.df.iloc[i].time_start,i])



    def FreeCarAvailable(self, row, request_time, request_pos):

        ## this function finds the nearest available car to che place of the call and simulate the trips

        # Compute the distances for all the free cars
        lats = [i[0] for i in self.free_cars]
        lons = [i[1] for i in self.free_cars]
        soc = [i[7] for i in self.free_cars]
        keys = [i[16] for i in self.free_cars]
        for i in self.free_cars_recharging:
            lats.append(i[0])
            lons.append(i[1])
            soc.append(i[7])
            keys.append(i[16])
        distances = 1.5 * haversine_distance(lats, lons, request_pos)
        # Choose the closest vehicle and send it to the user
        distance_to_user = distances.min()
        indices = [i for i, x in enumerate(distances) if x == distance_to_user]
        if len(indices)>1: #two or more available cars at the same charging station
            #let's find the car with greatest soc
            soc_same_place = [soc[i] for i in indices]
            chosen_vehicle = indices[np.argmax(soc_same_place)]
        else:
            chosen_vehicle = np.argmin(distances)

        # This car has to go to the user
        dt_to_user = 3600*distance_to_user/self.L3VehicleSpeed #this is a time in seconds

        if dt_to_user/60 > self.USER_WAIT_FOR_CAR: # If the user has to wait too much, the call is dropped
            self.skipped_calls.append(1)
        else:
            self.user_wait_time.append(dt_to_user)
            self.skipped_calls.append(0)
            # Now the user has the car and can use it
            dt_trip = (row.time_end - row.time_start).delta/1e9
            # now check whether the car was recharging or not
            N_free_cars = len(self.free_cars)
            if chosen_vehicle<N_free_cars:
                used_car = self.free_cars.pop(chosen_vehicle)
            else:
                used_car = self.free_cars_recharging.pop(chosen_vehicle-N_free_cars)

            if used_car[8] == True: #charging=true --> I free the charging stations
                used_car = self.FreeChargingStation(used_car,time_req=request_time)
                dead_time = 0

            else:
                # save the dead time (time when the vehicle was unused)
                dead_time = (request_time - used_car[2]).delta / 1e9



            # Now update the car statistics
            new_soc = self.newSOC(used_car[7],charging=False,tot_distance=row.distance+distance_to_user)
            new_time = request_time + pd.to_timedelta((dt_to_user + dt_trip), unit = 's')
            used_car[13].append(new_soc)
            used_car[14].append(new_time)
            used_car[15]=new_time

            used_car[0] = row.latitude_end
            used_car[1] = row.longitude_end
            used_car[2] = new_time
            used_car[3] = used_car[3] + row.distance + distance_to_user
            used_car[4] = used_car[4] + distance_to_user
            used_car[5] = used_car[5] + dead_time/3600
            used_car[6] = used_car[6] + 1
            used_car[7] = new_soc
            used_car[8] = False
            used_car[11] = used_car[11]
            used_car[15] = new_time

            self.insertOrderedVehicle(used_car,busy_vehicle=True) ## insert the vehicle in the busy_car list

    def NoFreeCar(self, row, request_time, request_pos):

        # There are no free cars
        self.skipped_calls.append(1)



    def CheckFreeVehicles(self, request_time):
        # This function updates all the vehicles' statistics until
        # 'request time'(time of the most recent call in the simulation process)
        # and than check if there are free vehicles available to serve the call
        #### implementation using list

        # counter needed in order to know the number of elements(vehicles) modified, so that it is possible
        # to reorder the list at the end of the loop
        counter_modified_vehicles = 0
        i = 0
        while i < len(self.busy_cars): ##update vehicles in busy_car list
            vehicle = self.busy_cars[i]  # lists are copied by reference!!!!
            if vehicle[15] > request_time:
                # new_need_check > request time
                # no more need to check other cars
                break
            param_car = vehicle  # [copied by reference] the elements in self.busycars are modified as well
            if param_car[8] == True and param_car[12] < request_time:
                # the vehicle is recharging, and its SOC has to be updated

                time_delta = (request_time - param_car[12]).total_seconds()
                delta_second_check = 900 ## the SOC level of the cars is updated
                # every 900 seconds when recharing
                if time_delta > 0:
                    param_car[12] = request_time  # update new last_check_recharge
                    param_car[7] = self.newSOC(param_car[7], True, delta_time=time_delta)
                    param_car[10] = param_car[10] + time_delta / 3600  # time spent recharging, in hours

                    #keep track of soc evolution of the vehicle
                    param_car[13].append(param_car[7])
                    param_car[14].append(param_car[12])

                    param_car[15] = param_car[12]
                    if param_car[7] == 100:  # the car is completely charged, leave the charging station
                        param_car[15] = param_car[12]
                        param_car = self.FreeChargingStation(param_car)
                        car = self.busy_cars.pop(i)
                        i -= 1  # with pop one element removed in the list
                        self.free_cars.append(car)
                    elif param_car[7] > self.threshold_car_free: # the SOC level is now over
                        # the high_threshold, but the car still remains in charge

                        param_car[15] = param_car[12] + datetime.timedelta(seconds=delta_second_check)

                        #move vehicle from busy_car list to free_car_recharging list
                        car = self.busy_cars.pop(i)
                        i -= 1  # with pop one element removed in the list
                        self.insertOrderedVehicle(car, busy_vehicle=False)
                    else: #the car remains busy, update new time when it has to be checked again
                        param_car[15] = param_car[12] + datetime.timedelta(seconds=delta_second_check)
                        counter_modified_vehicles += 1

            else:

                if param_car[2] <= request_time: # the vehicle has finished a trip
                    if param_car[7] > self.threshold_send_recharge:
                        # soc greater than lower threshold = 25%/60%,
                        # vehicle is free and remains in the position where the trip ended

                        #move car from busy list to free_cars list
                        car = self.busy_cars.pop(i)
                        i -= 1
                        self.free_cars.append(car)
                    else:
                        # else the car must be sent to recharge
                        if self.every_station_occupied == True:
                            param_car[15] = param_car[15] + datetime.timedelta(
                                seconds=600)  # every station occupied, wait 10 minutes
                            counter_modified_vehicles += 1

                        else:
                            #send the car to the charging station
                            used_car, available = self.SendToChargingStation(param_car, request_time)
                            if available == True:
                                # there's at least one station free
                                # this is needed to deal with the limit case of the car going to
                                # occupy the last available station

                                # update time waiting to be sent to charging station
                                used_car[9] = used_car[9] + (request_time -
                                                             used_car[2]).total_seconds() / 3600
                                counter_modified_vehicles += 1
                            else:
                                # the previous car occupied the last station, no more free
                                param_car[15] = param_car[15] + datetime.timedelta(
                                    seconds=600)  # every station occupied, wait 10 minutes
                                counter_modified_vehicles += 1

            i += 1
        ## reorder the elements modified in the list
        self.reOrderFrontVehicles(counter_modified_vehicles, True)

        # now update free cars recharging
        counter_modified_vehicles = 0
        i = 0
        while i < len(self.free_cars_recharging):
            vehicle = self.free_cars_recharging[i]
            if vehicle[15] > request_time:
                # new_need_check > request time
                # no more need to check other cars
                break
            param_car = vehicle
            if param_car[12] < request_time:
                # if we passed the last check
                time_delta = (request_time - param_car[12]).total_seconds()
                delta_second_check = 900  # next update after 15 minutes
                if time_delta > 0:
                    param_car[12] = request_time  # update new last_check_recharge
                    param_car[7] = self.newSOC(param_car[7], True, delta_time=time_delta)
                    param_car[10] = param_car[10] + time_delta / 3600  # time spent recharging, in hours
                    param_car[13].append(param_car[7])
                    param_car[14].append(param_car[12])
                    if param_car[7] == 100:  # the car is completely charged, leave the charging station
                        param_car = self.FreeChargingStation(param_car)
                        car = self.free_cars_recharging.pop(i)
                        i -= 1
                        self.free_cars.append(car)
                    else:
                        param_car[15] = param_car[12] + datetime.timedelta(seconds=delta_second_check)
                        counter_modified_vehicles += 1
            i += 1

        self.reOrderFrontVehicles(counter_modified_vehicles, False)

        ### now update free vehicles
        counter_updated_vehicles = 0
        if self.night == True and self.every_car_to_recharge == False:
            # when night comes, recharge policy change-> send free cars under thres to recharge
            i = 0
            while i < len(self.free_cars):
                vehicle = self.free_cars[i]
                if vehicle[7] < self.threshold_send_recharge:
                    vehicle[15] = request_time
                    car = self.free_cars.pop(i)
                    i -= 1
                    self.insertOrderedVehicle(car, True)
                    counter_updated_vehicles += 1
                i += 1
            if counter_updated_vehicles == 0:
                self.every_car_to_recharge = True

    def reOrderFrontVehicles(self,number_modified_cars,busy_list):
        #takes as input the number of car in the front of the list that must be reordered

        #reorder the list
        if busy_list == True: #busy_car_list
            list_mod_cars=[]
            while number_modified_cars>0:
                #remove cars from the list and add again in the correct position
                list_mod_cars.append(self.busy_cars.pop(0))
                number_modified_cars -=1
            while len(list_mod_cars)>0:
                vehicle = list_mod_cars.pop(0)
                self.insertOrderedVehicle(vehicle,True)

        if busy_list == False: #free_recharging_car_list
            list_mod_cars = []
            while number_modified_cars > 0:
                #remove cars from the list and add again in the correct position
                list_mod_cars.append(self.free_cars_recharging.pop(0))
                number_modified_cars -= 1
            while len(list_mod_cars) > 0:
                vehicle = list_mod_cars.pop(0)
                self.insertOrderedVehicle(vehicle, False)





    def insertOrderedVehicle(self,vehicle,busy_vehicle):

        ## this function inserts the vehicle passed as input in the correct position in the list,
        # according to new_need_check parameter, so the list remains always ordered.


        if busy_vehicle==True: #busy_car_list
            if len(self.busy_cars)==0:
                self.busy_cars.append(vehicle)
                return
            new_check = vehicle[15]
            keys = [car[15] for car in self.busy_cars]
            index = bisect.bisect(keys, new_check) #find the index of the correct position
            self.busy_cars.insert(index, vehicle)


        if busy_vehicle == False: #free vehicle recharging list
            if len(self.free_cars_recharging) == 0:
                self.free_cars_recharging.append(vehicle)
                return

            new_check = vehicle[15]

            keys = [car[15] for car in self.free_cars_recharging]
            index = bisect.bisect(keys, new_check)
            self.free_cars_recharging.insert(index, vehicle)


    def newSOC(self, old_soc, charging, *args, **kwargs):
        #this function gives as output the new State of Charge of the car in percentage terms
        delta_time_in_sec = kwargs.get('delta_time',None) #in seconds
        distance = kwargs.get('tot_distance',None) #in km
        new_soc = 0
        if charging == True: #tha car is recharging
            charge_battery = self.efficiency_battery * self.recharging_power * delta_time_in_sec/3600
            pct_charge = charge_battery/self.battery_capacity
            new_soc = old_soc + pct_charge*100 ## new soc level
            if new_soc>100:
                new_soc=100
        else: #i'm using the car->discharging
            pct_used_power = self.consumption * distance/self.battery_capacity
            new_soc = old_soc - pct_used_power*100
            if new_soc<0:
                new_soc=0
                print('the charge wouldn\'t be enuogh to complete the trip')

        return new_soc





    def createData(self, N):
    #create summary output of the simulation
        v0, v1, v2, v3, v4, v5, v6, v7 = [], [], [], [], [], [], [], []
        self.evolution_soc=[]
        self.time_soc=[]

        for car in self.free_cars+self.free_cars_recharging+self.busy_cars:
            v0.append(car[16])
            v1.append(car[0])
            v2.append(car[1])
            v3.append(car[2])
            v4.append(car[3])
            v5.append(car[4])
            v6.append(car[5])
            v7.append(car[6])
            self.final_df.loc[len(self.final_df)] = [car[16],car[0],car[1],car[2],car[3],
                                                     car[4],car[5],car[6],car[9],car[10],car[11]]
            self.evolution_soc.append(car[13])
            self.time_soc.append(car[14])

    def Simulation(self, N):
        # This function performs the simulation process
        self.InitializeCars(N)

        users_wait_time, free_cars, skipped_calls = [], [], []

        # data of a trip
        info_col = ['time_start', 'latitude_start', 'longitude_start', 'time_end',
                    'latitude_end', 'longitude_end', 'distance']

        # scroll through the dataframe of the trips
        for index, row in tqdm(self.df[info_col].iterrows(), total = len(self.df)):

            # When and where the call has been received
            request_time = row.time_start
            request_pos = np.array([row.latitude_start, row.longitude_start])

            #### changes in recharging thresholds
            if self.night == False and (request_time.hour>=23 or request_time.hour<7): #night came
                self.threshold_car_free = self.threshold_car_free_night
                self.threshold_send_recharge = self.threshold_send_recharge_night
                self.night = True
                self.every_car_to_recharge = False #needed in check free vehicles

            if self.night == True and (request_time.hour>=7 and request_time.hour<23): #morning glory
                self.threshold_car_free = self.threshold_car_free_day
                self.threshold_send_recharge = self.threshold_send_recharge_day
                self.night=False

            # Find the free vehicle at this moment in time
            self.CheckFreeVehicles(request_time)
            self.free_cars_v.append(len(self.free_cars)+len(self.free_cars_recharging))

            # Look for the cars and see if one is free
            if (len(self.free_cars)+len(self.free_cars_recharging)) > 0:
                # Free vehicles are available, find the closest to the user
                self.FreeCarAvailable(row, request_time, request_pos)
            else:
                # There are no free vehicles!
                self.NoFreeCar(row, request_time, request_pos)


        ## here the simulation is completed, let's create some summary data and send it as outputs

        time_span_simulation = [self.df.time_start.min(),self.df.time_end.max()]

        self.createData(N)
        return self.final_df, self.user_wait_time, self.free_cars_v, self.skipped_calls, \
               time_span_simulation, self.evolution_soc,self.time_soc

    def Run(self):

        ## this function runs the simulation
        ## a bit useless but remains from a previous version of the simulator
         if self.N_PARALLEL == 1:
            res = [self.Simulation(self.RANGE_TO_TEST[0])]

         return res

    def ShowInfo(self,path):

        ## this function displays info of the current simulation and save them also in a txt file
        print('--- Simulator Settings ---')
        print('Dataset imported from: {}'.format(self.path))
        print('Desired range of analysis: {}'.format(self.RANGE_TO_TEST))
        if self.EARLY != -1:
            print('Early stop: {}'.format(self.EARLY))
        if self.N_PARALLEL != -1:
            print('Parallel processes: {}'.format(self.N_PARALLEL))
        print('\n--- Dataset Info ---')
        print('Simulated trips: {}'.format(len(self.df)))
        print('Simulation start: {}'.format(self.df.time_start.min()))
        print('Simulation end: {}'.format(self.df.time_end.max()))
        print(self.df.time_end.idxmax())
        print('Time span: {}'.format(self.df.time_end.max() - self.df.time_start.min()))
        appendNewLine(path, '--- Simulator Settings ---')
        appendNewLine(path, 'Dataset imported from: ' + str(self.path))
        appendNewLine(path,' ')
        appendNewLine(path,'--- Dataset Info ---')
        appendNewLine(path,'Simulated trips: '+ str(len(self.df)))
        appendNewLine(path,'Simulation start: '+ str(self.df.time_start.min()))
        appendNewLine(path, 'Simulation end: ' + str(self.df.time_start.max()))
        appendNewLine(path,'Time Span: '+str(self.df.time_end.max() - self.df.time_start.min()))
        appendNewLine(path, ' ')





    def SetupEarlyStop(self, EARLY):
        ## used to stop early the simulation (until the EARLYth trip)
        self.df = self.df[:EARLY]
        self.EARLY = EARLY


    def SetupBeginSimulation(self, date):
        ## setup beginning of the simulation by date
        initial_date = pd.Timestamp(datetime.datetime.strptime(date, "%Y%m%d %H:%M:%S"))
        self.df = self.df.loc[(self.df['time_start'] >= initial_date)]

    def SetupEndSimulation(self, date):
        ## setup end of the simulation by date
        final_date = pd.Timestamp(datetime.datetime.strptime(date, "%Y%m%d %H:%M:%S"))
        self.df = self.df.loc[(self.df['time_end'] <= final_date)]

    def MultiplicationData(self, times):
        # this function puts together more weeks of trip data
        pd.options.mode.chained_assignment = None  # default='warn' ## avoid some useless warnings

        begin_first_week = self.df.iloc[0].time_start
        end_first_week = begin_first_week + datetime.timedelta (days=7)
        df_list = []
        df_list.append(self.df.loc[(self.df['time_start'] >= begin_first_week) &
                                   (self.df['time_end'] <= end_first_week)])
        for i in range(times-1):
            df_temporary = self.df.loc[(self.df['time_start'] >= begin_first_week +
                                        datetime.timedelta(days=7*(i+1)) ) &
                                       (self.df['time_end'] <= end_first_week +
                                        datetime.timedelta(days=7*(i+1)))]

            ## here all the trips in a specific week are modified in order
            # to be overlapping with the first week
            df_temporary['time_start'] = df_temporary['time_start'].\
                apply(lambda x: x-datetime.timedelta(days=7*(i+1)))
            df_temporary['time_end'] = df_temporary['time_end'].\
                apply(lambda x: x - datetime.timedelta(days=7 * (i+1)))
            df_list.append(df_temporary)

        ## concatenate all the weeks of trips and order by time_start
        self.df = pd.concat(df_list).sort_values(by= 'time_start').reset_index()
        pd.options.mode.chained_assignment = 'warn'  # default='warn'


# ---------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------

def haversine_distance(lats, lons, request_pos):
    # compute haversine distance

    # convert decimal degrees to radians
    lat1, lon1 = np.radians(lats), np.radians(lons)
    lat2 = np.radians([request_pos[0] for _ in range(len(lats))])
    lon2 = np.radians([request_pos[1] for _ in range(len(lats))])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371
    return c*r


def haversine(lon1, lat1, lon2, lat2):
    #haversine distance but with different inputs
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r


def evaluation_opt_function(df,user_wait,skipped_calls,time_span,n_cars,n_stations,path):

    ## function used to evaluate the objective function value
    ### parameters used in the objective function
    cost_car = 35000 #euros
    discount = 0.30
    lifetime = 5 #in years
    maintenance = 1000 #euros per year
    price_energy = 0.30 #euros per Kwh
    station_cost = 5000
    parking_rental = 500 #euros per year

    ## compute useful statistics (dependent variables) of the current simulation
    frac_skipped_calls = sum(skipped_calls)/len(skipped_calls)
    time_sim = time_span[1]-time_span[0]
    time_sim = time_sim.total_seconds()/(24*3600)
    avg_distance_per_day_per_car = df['distance'].sum()/(len(df)*time_sim)
    trips_per_day = df['tot_calls'].sum()/(len(df)*time_sim) #per car
    frac_overhead = df['overhead'].sum()/(df['distance'].sum()+df['overhead'].sum())
    avg_wait = sum(user_wait) / len(user_wait) /60 #wait in minutes

    ## compute costs
    fleet_cost = n_cars*(cost_car*(1-discount)+maintenance*lifetime)
    energy_cost = df['time_recharging'].sum() * 13 * (365*lifetime*price_energy)/time_sim
    charging_stations_cost = n_stations*(station_cost+parking_rental*lifetime)

    ## objective function value
    function_val = (fleet_cost+energy_cost+charging_stations_cost)/1000000 #in million of euro

    ## let's check if the current solution respects the constraints of the optimization problem


    ### variables used in order to manage the solutions which don't respect the constraints
    base_millions=40
    multiplicative_factor=function_val/base_millions

    if frac_skipped_calls>0.05:
        ### add an exponential penalty in function value if the constraint is not respected
        function_val = function_val + exp((frac_skipped_calls - 0.05) * 100) * multiplicative_factor
        appendNewLine(path, 'Too many skipped calls')
    if avg_distance_per_day_per_car < 150:
        ### penalty if the constraint is not respected
        function_val = function_val + 1000
        appendNewLine(path, 'Not enough km per day per car')
    if frac_overhead > 0.3:
        ### penalty if the constraint is not respected
        function_val = function_val + 1000
        appendNewLine(path, 'Too much overhead')
    if avg_wait > 6:
        ### penalty if the constraint is not respected
        function_val=function_val+(avg_wait-6)*10
        appendNewLine(path, 'Average wait too big')

    return function_val


def objective_fun(x,radius):
    ### This function perform an iteration in the optimization process
    # and returns as output the value of the objective function

    global numero_iterazioni #total number of iterations

    # path where to save output of this process
    path_save_data = r'...\Risultato_radius'+str(radius)+'.txt'

    n_cars = x[0] #cars
    n_charging_stations = x[1]

    integer_n_cars = [int(round(n_cars,0))] #number of vehicles
    index_closest_radius = find_closest_index_radius(radius)
    radius_considered = range_radius[index_closest_radius] #radius of the coverage area
    integer_n_stations = float(round(n_charging_stations,0)) #number of the charging stations

    #setup the simulation class
    sim = Simulator(paths[index_closest_radius], integer_n_cars, radius_considered, integer_n_stations)

    #setup simulation parameters
    begin_simulation_date = "20190107 00:00:00"  # 'YYYYMMDD hh:mm:ss' format string
    sim.SetupBeginSimulation(begin_simulation_date)
    sim.MultiplicationData(multiplication_data_variable[index_closest_radius])
    end_simulation_date = "20190114 00:00:00"  # 'YYYYMMDD hh:mm:ss' format string
    sim.SetupEndSimulation(end_simulation_date)
    if numero_iterazioni==0:
        sim.ShowInfo(path_save_data)

    #run simulation
    result = sim.Run()

    #collect results of the simulation
    res = result[0]
    df = res[0]
    user_wait = res[1]
    free_cars = res[2]
    skipped_calls = res[3]
    time_span = res[4]
    evolution_soc = res[5]
    time_soc = res[6]



    numero_iterazioni+=1

    ## print variables in order to keep track of the optimization evolution
    print('Iterazione numero: ',numero_iterazioni)
    print('N AUTO: ', integer_n_cars,'____', n_cars)
    print('RAGGIO: ', range_radius[index_closest_radius],'____', radius)
    print('N Stazioni: ', integer_n_stations)
    val_function = evaluation_opt_function(df, user_wait, skipped_calls, time_span,
                                           integer_n_cars[0],integer_n_stations,path_save_data)
    appendNewLine(path_save_data, 'N AUTO: '+str(integer_n_cars) + '____'+ str(n_cars)+' '+
                  'N STAZIONI: '+str(integer_n_stations)+' '+'Objective fun Value :'+str(val_function)+
                  'Iterazione numero: '+str(numero_iterazioni))
    print('Valore funzione ottimo: ', val_function)
    print()

    return val_function


def find_closest_index_radius(raggio):
    ## find the corresponding index in range_radius, given a radius value
    index = 0
    while raggio>range_radius[index]:
        index += 1
    if raggio == range_radius[index]:
        return index
    else:
        dist1 = raggio - range_radius[index-1]
        dist2 = range_radius[index] - raggio
        if dist1<dist2:
            return index-1
        else:
            return index

def run_parallel_optimizations(indexes_radius_to_test):

    ### function used to run parallel optimizations, given different radius of the coverage area
        t_s = time.time()
        N_PARALLEL = mp.cpu_count()
        if len(indexes_radius_to_test)>N_PARALLEL:
            print('more parallel simulations than the number of cores')


        p = mp.Pool(len(indexes_radius_to_test))
        print('Running {} simulations in parallel'.format(len(indexes_radius_to_test)))
        res = p.map(minimization_function,indexes_radius_to_test)
        p.close()
        t_e = time.time()
        print('Done in {:.1f}s'.format(t_e - t_s))
        return res




def appendNewLine(file_name, text_to_append):
    # this function appends text as a new line at the end of file
    # Open the file in append & read mode ('a+')
    with open(file_name, "a+") as file_object:
        # Move read cursor to the start of file.
        file_object.seek(0)
        # If file is not empty then append '\n'
        data = file_object.read(100)
        if len(data) > 0:
            file_object.write("\n")
        # Append text at the end of file
        file_object.write(text_to_append)



def minimization_function(local_index_radius):
    ### This function starts the optimization process (in this case minimization of the objective function)
    actual_solution = [2000, 1200] ## initial point (not needed if the initial simplex is specified)

    ### here the initial simplex for 3 (10km,11km,12km of radius) parallel
    # optimizations is set (add for more parallel simulations)
    if local_index_radius == 8:
        #10 km
        simplesso_iniziale = np.array([[3700, 1300], [3790, 1000], [3740, 1122]])
    if local_index_radius == 12:
        #11km
        simplesso_iniziale = np.array([[4900, 1200], [4800, 1400], [4830, 1350]])
    if local_index_radius == 16:
        #12km
        simplesso_iniziale = np.array([[5700, 2400], [5750, 1900], [5800, 1800]])

    bounds_cars = (100, 20000)
    bounds_n_charging = (50, 10000)
    bounds = [bounds_cars, bounds_n_charging]
    radius = range_radius[local_index_radius]

    ## call the optimization function, using Nelder-Mead algorithm
    res = sc_opt.minimize(objective_fun, actual_solution, args=(radius,),method='Nelder-Mead', bounds=bounds,
                          options={'initial_simplex':simplesso_iniziale})
    return res

# ---------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------

#### GLOBAL VARIABLES

DATA_DIR = r'...\dfs_brescia_radius' ## directory containg the trips dataset, one dataset for each
# radius of the coverage area


## this code was used for optimization for a radius of the coverage area from 8 km to 12km
##(with little modifications it was adapted for bigger radius)
range_radius = np.linspace(8,12,17).tolist()
range_radius = [round(i,2) for i in range_radius]
files = os.listdir(DATA_DIR)  #all files in the directory
paths = [os.path.join(DATA_DIR, file) for file in files] #create all paths, list format
paths = paths[9:]+paths[:9] #reorder the paths


multiplication_data_variable = [43,43,43,44,44,44,44,45,45,45,44,44,43,43,42,42,42]
##used to unify more weeks of data

numero_iterazioni=0

### finally main of the code
if __name__ == '__main__':
    optimal_val = 100000 ### initialize obective function to a huge value
    miglior_numero_cars = 0
    miglior_raggio = range_radius[0]
    miglior_index = 0
    converged = False
    ### here select a list of indexes (referred to range_radius list) w.r.t
    # which perform parallel optimizations
    indici_radius = [8,12,16]

    results = run_parallel_optimizations(indici_radius) ## run parallel optimizations
    # on different cores of the computer








