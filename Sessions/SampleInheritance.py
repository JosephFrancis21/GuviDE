# Inheritance 
class Father():
    def __init__(self):
        self.name = "Father"
    
    def Nose(self):
        return "Pointed Nose"
    
class Mother():
    def __init__(self):
        self.name = "Mother"

    def Patience(self):
        return "Patience"
    
# Single Inheritance
class Son(Father):  # Inheritance from Father
    def __init__(self):
        super().__init__() # To initalize the parent or Super Class
        self.name = "Son"
    
    def Hair(self):
        return "Black Hair"

# Multiple Inheritance
class Daughter(Father, Mother):
    def __init__(self):
        super().__init__()
        self.name = "Daughter"
    
    def Hair(self):
        return "Long Hair"
    
# Multilevel Inheritance
class GrandSon(Daughter):
    def __init__(self):
        self.name = "Grandson"
    
    def Eyes(self):
        return "Almond Eyes"
    

print("Imported SampleInheritance module")