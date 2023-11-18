import unittest
from controller.controller import *


class TestOp(unittest.TestCase):
    def test_byte_mul(self):
        self.assertEqual(byte_multiple(2, 1), b'\x02')
        self.assertEqual(byte_multiple(2, 2), b'\x04')
        self.assertEqual(byte_multiple(2, 4), b'\x08')

        self.assertEqual(byte_multiple(2, 4), b'\x08')


        self.assertEqual(byte_multiple(b'\x8d', b'a'), bit_wise_add(
            bit_wise_add(
                byte_multiple_by_g0_p(b'a', 7),
                byte_multiple_by_g0_p(b'a', 3)
            ),
            bit_wise_add(
                byte_multiple_by_g0_p(b'a', 2),
                b'a'
            )
        ))
        self.assertEqual(byte_multiple(b'a', b'\x8d'), byte_multiple(b'\x8d', b'a'))

    def test_byte_power(self):
        self.assertEqual(byte_power(2, 1), b'\x02')
        self.assertEqual(byte_power(2, 2), b'\x04')
        self.assertEqual(byte_power(2, 3), b'\x08')
        self.assertEqual(byte_power(2, 4), b'\x10')
        self.assertEqual(byte_power(2, 5), b'\x20')
        self.assertEqual(byte_power(2, 6), b'\x40')
        self.assertEqual(byte_power(2, 7), b'\x80')
        self.assertEqual(byte_power(2, 8), b'\x1d')

    def test_vector_multiple(self):
        # b'holloni0iv98kq3ebtw8oa5ryiktsd71x7qa09p7k6csrggokn7g6ug55jnaunxy1xrzengrgf5s5de6rpr8olvzqbuhzkjym0g3b88miqi1im7xxn6toomr7cyoq95h'
        pass



if __name__ == '__main__':
    unittest.main()