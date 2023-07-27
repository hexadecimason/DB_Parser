class OPIC_Well:
    from OPIC_Well import OPIC_WellBox
    
    fileNumber = ''
    boxCount : int = 0
    boxes = [OPIC_WellBox]
    api : int = 0
    operator = ''
    leaseName = ''
    wellNum = ''
    STR = []
    QQ = ''
    latLong = []
    county = ''
    state = ''
    field = ''
    well_top = 1 # start with wrong values for debugging
    well_bottom = 0

    def __init__(self, file, boxes, api, op, lease, well, sec, t, td, r, rd, qq, lat, long, co, st, field):
        self.fileNumber = file
        self.boxCount = boxes
        self.api = api
        self.operator = op
        self.leaseName = lease
        self.wellNum = well
        self.STR = [sec, t, td, r ,rd]
        self.QQ = qq
        self.latLong = [lat, long]
        self.county = co
        self.state = st
        self.field = field
        self.boxes = []
    
    # boxNum, top, bottom, fm, dia, sType, bType, cond, rest, com
    def addBox(self, number, top, bottom, fm, dia, sType, bxType, cond, rest, comments):
        bx = OPIC_WellBox(number, top, bottom, fm, dia, sType, bxType, cond, rest, comments)
        (self.boxes).append(bx)
        self.updateWellInterval()
        self.boxCount = len(self.boxes) #= len(self.boxes)

    def updateWellInterval(self):
        if len(self.boxes) == 0:
            self.boxes = ["box level data not available"]
            self.well_top = 0
            self.well_bottom = 0
            return
        self.well_top = (self.boxes[0]).top
        self.well_bottom = (self.boxes[-1]).bottom
        return

    def __str__(self) -> str:
        s = 'File: ' + str(self.fileNumber) + ' | Well: ' + str(self.operator) + ' ' + str(self.wellNum) + ' ' + str(self.leaseName) + ' | API: ' + str(self.api) + '\n'
        s = s + 'Location: ' + str(self.STR) + ', ' + str(self.QQ) + ' | ' + str(self.latLong) + ' | ' + str(self.county) + ', ' + str(self.state)
        s = s + ' (' + str(self.field) + ' Field)\n'
        s = s + 'Boxes: ' + str(self.boxCount) + ' | Top: ' + str(self.well_top) + ' | Bottom: ' + str(self.well_bottom) + '\n'
        s = s + '-------------------------------'

        #for i in range(len(self.boxes)):
        #    s = s + str(self.boxes[i]) + '\n'

        #s = s + '-------------------------------\n'

        return s





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