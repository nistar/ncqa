from datetime import datetime


class OptionalExclusions:

    def __init__(self):
        self.UnilateralMastectomyLeft = []
        self.UnilateralMastectomyRight = []
        self.UnilateralMastectomy = []
        self.BilateralMastectomy = []
        self.BilateralModifier = []
        self.LeftModifier = []
        self.RightModifier = []
        self.HistoryOfBilateralMastectomy = []
        self.AbsenceOfLeftBreast = []
        self.AbsenceOfRightBreast = []

    def history_of_bilateral_mastectomy(self) -> bool:
        return bool(self.HistoryOfBilateralMastectomy)

    def bilateral_mastectomy(self) -> bool:
        return len(self.BilateralMastectomy) > 0

    def unilateral_mastectomy_with_bilateral_mod(self) -> bool:
        return len(self.UnilateralMastectomy) > 0 and len(self.BilateralModifier) > 0

    def has_modifiers(self) -> bool:
        if bool(self.LeftModifier) or bool(self.RightModifier) or bool(self.BilateralModifier):
            return True
        return False

    def two_unilateral_mastectomies_without_mod(self) -> bool:

        if len(self.UnilateralMastectomy) == 2 and not self.has_modifiers():
            # 14 days or more apart
            return (self.UnilateralMastectomy[-1] - self.UnilateralMastectomy[0]).days >= 14

        if len(self.UnilateralMastectomy) > 2:
            print('More than Two Unilateral Mastectomies')

        return False

    def unilateral_mastectomy_without_mod_and_right_mastectomy(self) -> bool:

        if bool(self.UnilateralMastectomyRight):
            return True

        mastectomy_no_mod = False
        mastectomy_with_mod = False

        if len(self.UnilateralMastectomy) == 2:
            for dt in self.UnilateralMastectomy:
                if dt in self.RightModifier:
                    mastectomy_with_mod = True
                else:
                    mastectomy_no_mod = True

            return mastectomy_with_mod and mastectomy_no_mod and \
                (self.UnilateralMastectomy[-1] - self.UnilateralMastectomy[0]).days >= 14

        return False

    def unilateral_mastectomy_without_mod_and_left_mastectomy(self) -> bool:

        if bool(self.UnilateralMastectomyLeft):
            return True

        mastectomy_no_mod = False
        mastectomy_with_mod = False

        if len(self.UnilateralMastectomy) == 2:
            for dt in self.UnilateralMastectomy:
                if dt in self.LeftModifier:
                    mastectomy_with_mod = True
                else:
                    mastectomy_no_mod = True

            return mastectomy_with_mod and mastectomy_no_mod and \
                (self.UnilateralMastectomy[-1] - self.UnilateralMastectomy[0]).days >= 14

        return False

    def left_and_right_mastectomy(self) -> bool:
        return self.left_mastectomy() and self.right_mastectomy()

    @staticmethod
    def unilateral_mastectomy_with_mod_same_visit(mastectomy: list, modifier: list) -> bool:
        return bool(set(mastectomy).intersection(modifier))

    def left_mastectomy(self) -> bool:
        if bool(self.AbsenceOfLeftBreast) or bool(self.UnilateralMastectomyLeft):
            return True

        if bool(self.LeftModifier):
            return self.unilateral_mastectomy_with_mod_same_visit(self.UnilateralMastectomy, self.LeftModifier)

        return False

    def right_mastectomy(self) -> bool:
        if bool(self.AbsenceOfRightBreast) or bool(self.UnilateralMastectomyRight):
            return True

        if bool(self.RightModifier):
            return self.unilateral_mastectomy_with_mod_same_visit(self.UnilateralMastectomy, self.RightModifier)

        return False

    def __str__(self):
        output = ''

        if self.BilateralMastectomy:
            output += ' Bilateral Mastectomy '
            for dat in self.BilateralMastectomy:
                output += datetime.strftime(dat, '%Y-%m-%d') + ' '

        if self.UnilateralMastectomyRight:
            output += ' Unilateral Mastectomy Right '
            for dat in self.UnilateralMastectomyRight:
                output += datetime.strftime(dat, '%Y-%m-%d') + ' '

        if self.UnilateralMastectomyLeft:
            output += ' Unilateral Mastectomy Left '
            for dat in self.UnilateralMastectomyLeft:
                output += datetime.strftime(dat, '%Y-%m-%d') + ' '

        if self.UnilateralMastectomy:
            output += ' Unilateral Mastectomy ['
            for dt in self.UnilateralMastectomy:
                output += datetime.strftime(dt, '%Y-%m-%d')
                if dt in self.LeftModifier:
                    output += ' With Left Modifier '
                elif dt in self.RightModifier:
                    output += ' With Right Modifier '
            output = output[0: -1]
            output += '] '

        if self.HistoryOfBilateralMastectomy:
            output += ' History Of Bilateral Mastectomy '

        if self.BilateralModifier:
            output += ' With Bilateral Modifier '

        return output

    def empty(self) -> bool:
        return len(self.UnilateralMastectomyLeft) == 0 and \
               len(self.LeftModifier) == 0 and \
               len(self.BilateralModifier) == 0 and \
               len(self.RightModifier) == 0 and \
               len(self.BilateralMastectomy) == 0 and \
               len(self.UnilateralMastectomy) == 0 and \
               len(self.HistoryOfBilateralMastectomy) == 0 and \
               len(self.UnilateralMastectomyRight) == 0

