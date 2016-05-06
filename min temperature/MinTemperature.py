from mrjob.job import MRJob

class MinTemperature(MRJob):
    
    def MakeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fehrenheit = celsius * 1.8 +32.0
        return fehrenheit
        
    def mapper(self, _, line):
        (location, date, type, data, x, y, z, w) = line.split(',')
        if (type == 'TMIN'):
            temperature = self.MakeFahrenheit(data)
            yield location, temperature
            
    def reducer(self, location, temps):
        yield location, min(temps)
        
if __name__ == '__main__':
    MinTemperature.run()