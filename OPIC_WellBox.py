class OPIC_WellBox:
    boxNumber = 0
    top = 0
    bottom = 0
    formation = 0
    diameter = 0
    sampleType = ''
    boxType = ''
    condition = ''
    restrictions = ''
    comments = ''

    def __init__(self, num, top, bottom, fm, dia, sType, bxType, cond, rest, comments):
        self.boxNumber = num
        self.top = top
        self.bottom = bottom
        self.formation = fm
        self.diameter = dia
        self.sampleType = sType
        self.boxType = bxType
        self.condition = cond
        self.restrictions = rest
        self.comments = comments
    
    def __str__(self) -> str:
        s = 'Box: ' + str(self.boxNumber) + ' | Top: ' + str(self.top) + ' | Bottom: ' + str(self.bottom) + ' | Fm: ' + str(self.formation) + '\n'
        s = s + '\t' + str(self.diameter) + '" ' + str(self.sampleType) + ', ' + str(self.condition) + ', ' + str(self.boxType)
        s = s + ' | ' + str(self.restrictions) + '\n\t' + str(self.comments)
        s = s + '\n'
        
        return s
    
    def __repr__(self) -> str:
        s = 'Box: ' + str(self.boxNumber) + ' | Top: ' + str(self.top) + ' | Bottom: ' + str(self.bottom) + ' | Fm: ' + str(self.formation) + '\n'
        s = s + '\t' + str(self.diameter) + '" ' + str(self.sampleType) + ', ' + str(self.condition) + ', ' + str(self.boxType)
        s = s + ' | Restrictions: ' + str(self.restrictions) + '\n\t' + str(self.comments)

        return s