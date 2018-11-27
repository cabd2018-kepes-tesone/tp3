import random
import time

SIMULATION_DELAY = 0.2
LATITUDE = 100
LONGITUDE = 100
MAX_VEL = 10
MAX_WAIT = 50
CANT_VEHICLES = 20
PLACE_PROB = 0.7
PLACES = ["Zoologico", "Shopping", "Plaza", "Museo", "Cine", "Teatro"]
OTHER_PLACE = "Otro"
CANT_PLACES = 50


class Vehicle:
    
    def __init__(this, _id, _places):
        this.places = _places
        this.id = _id
        this.lat = random.randint(1, LATITUDE)
        this.lon = random.randint(1, LONGITUDE)
        this.vel = random.randint(1, MAX_VEL)
        this.wait = random.randint(0, MAX_WAIT)
        this.place = ""
        this.move = MAX_VEL
        
        (this.des_lat, this.des_lon, this.des_place) = this.nextPlace()
        
    def step(this):
        if(this.wait > 0):
            this.wait = this.wait - 1
            this.place = ""
        else:
            this.move = this.move - this.vel
            if(this.move <= 0):
                if(this.lat > this.des_lat):
                    this.lat = this.lat - 1
                elif(this.lat < this.des_lat):
                    this.lat = this.lat + 1
                elif(this.lon < this.des_lon):
                    this.lon = this.lon + 1
                elif(this.lon > this.des_lon):
                    this.lon = this.lon - 1
                else:
                    this.wait = random.randint(1, MAX_WAIT)
                    this.place = this.des_place                    
                    (this.des_lat, this.des_lon, this.des_place) = this.nextPlace()
                    
                this.move = MAX_VEL + this.move
                return True
        return False
                
    def nextPlace(this):
        p = random.random()
        if(p < PLACE_PROB):
            p = random.randint(0, len(this.places)-1)
            return this.places[p]
        else:
            return (random.randint(1, LATITUDE), random.randint(1, LONGITUDE), OTHER_PLACE)
        
places = []
for i in range(CANT_PLACES):
    place = random.randint(0, len(PLACES)-1)
    places.append((random.randint(1, LATITUDE), random.randint(1, LONGITUDE), PLACES[place]))
        
vehicles = []
for i in range(CANT_VEHICLES):
    v = Vehicle(i+1, places)
    vehicles.append(v)

cur_time = 0
while True:
    for v in vehicles:
        if v.step():
            print(str(v.id) + ";" + str(v.lat) + ";" + str(v.lon) + ";" + str(cur_time) + ";" + v.place)
    cur_time = cur_time + 1
    time.sleep(SIMULATION_DELAY)
