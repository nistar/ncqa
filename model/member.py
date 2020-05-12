
class Member:
    def __init__(
            self,
            member_id: str,
            age: int,
            is_female: bool
    ):
        self.member_id = member_id
        self.age = age
        self.is_female = is_female

    def __repr__(self):
        gender = 'Female' if self.is_female else 'Male'
        return 'Age: {}. Member ID: {} Gender: {}'.format(
            self.age,
            self.member_id,
            gender
        )
