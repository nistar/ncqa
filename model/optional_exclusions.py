from datetime import datetime


class OptionalExclusions:

    def __init__(self):
        self.UnilateralMastectomyWithoutModifier = []
        self.UnilateralMastectomyWithLeftModifier = []
        self.UnilateralMastectomyWithRightModifier = []
        self.UnilateralMastectomyLeft = []
        self.UnilateralMastectomyRight = []

    def __str__(self):
        output = ''
        if self.UnilateralMastectomyWithoutModifier:
            output += ' UMast Without Mod '
            for dat in self.UnilateralMastectomyWithoutModifier:
                output += datetime.strftime(dat, '%Y-%m-%d')

        if self.UnilateralMastectomyWithLeftModifier:
            output += ' UMast With Left Mod '
            for dat in self.UnilateralMastectomyWithLeftModifier:
                output += datetime.strftime(dat, '%Y-%m-%d')

        if self.UnilateralMastectomyWithRightModifier:
            output += ' UMast With Right Mod '
            for dat in self.UnilateralMastectomyWithRightModifier:
                output += datetime.strftime(dat, '%Y-%m-%d')

        if self.UnilateralMastectomyRight:
            output += ' UMast Right '
            for dat in self.UnilateralMastectomyRight:
                output += datetime.strftime(dat, '%Y-%m-%d')

        if self.UnilateralMastectomyLeft:
            output += ' UMast Left '
            for dat in self.UnilateralMastectomyLeft:
                output += datetime.strftime(dat, '%Y-%m-%d')

        return output

    def empty(self) -> bool:
        return len(self.UnilateralMastectomyLeft) == 0 and \
               len(self.UnilateralMastectomyWithLeftModifier) == 0 and \
               len(self.UnilateralMastectomyWithRightModifier) == 0 and \
               len(self.UnilateralMastectomyWithoutModifier) == 0 and \
               len(self.UnilateralMastectomyRight) == 0

