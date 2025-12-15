import polars as pl


def add_frg_pl_pure(df):
    """ Adds "Beruf_frg" and "Ses_frg" variables to the dataset.
    """
  
    df = df.with_columns(
        pl.when((pl.col("RCEG") == 0) & (pl.col("BHBR") == 0) & (pl.col("QLGR") == 0)).then(0)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 0) & (pl.col("QLGR") == 9) & (
                (pl.col("VSGR") == 1) | (pl.col("VSGR") == 2))).then(1)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 1)).then(2)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 1)).then(3)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 1)).then(4)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 1)).then(5)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 1)).then(6)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 1)).then(7)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 1)).then(8)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 1)).then(9)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 1)).then(10)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 3) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 1)).then(11)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 3) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 1)).then(12)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 3) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 1)).then(13)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 3) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 1)).then(14)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 2)).then(15)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 2)).then(16)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 2)).then(17)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 2)).then(18)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 4) & (pl.col("VSGR") == 2)).then(19)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 5) & (pl.col("VSGR") == 2)).then(20)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 2)).then(21)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 7) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 1)).then(22)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 7) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 2)).then(23)
        .when(
            (pl.col("RCEG") == 1) & (pl.col("BHBR") == 8) & (pl.col("QLGR") == 0) & (pl.col("VSGR").is_in([1, 2]))).then(
            24)
        .when(
            (pl.col("RCEG") == 1) & (pl.col("BHBR") == 8) & (pl.col("QLGR") == 9) & (pl.col("VSGR").is_in([1, 2]))).then(
            25)
        .when(
            (pl.col("RCEG") == 1) & (pl.col("BHBR") == 0) & (pl.col("QLGR") == 9) & (pl.col("VSGR").is_in([5, 6]))).then(
            26)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 5)).then(27)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 5)).then(28)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 5)).then(29)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 5)).then(30)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 5)).then(31)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 5)).then(32)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 5)).then(33)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 5)).then(34)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 5)).then(35)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 3) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 5)).then(36)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 6)).then(37)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 6)).then(38)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 6)).then(39)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 6)).then(40)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 4) & (pl.col("VSGR") == 6)).then(41)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 6)).then(42)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 6)).then(43)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 6)).then(44)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 6)).then(45)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 6)).then(46)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 4) & (pl.col("VSGR") == 6)).then(47)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 6)).then(48)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 6)).then(49)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 6)).then(50)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 6)).then(51)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 6)).then(52)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 4) & (pl.col("VSGR") == 6)).then(53)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 5) & (pl.col("VSGR") == 6)).then(54)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 6)).then(55)
        .when((pl.col("RCEG") == 1) & (pl.col("BHBR") == 7) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 6)).then(56)
        .when(
            (pl.col("RCEG") == 2) & (pl.col("BHBR") == 0) & (pl.col("QLGR") == 9) & (pl.col("VSGR").is_in([1, 2]))).then(
            57)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 1)).then(58)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 1)).then(59)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 1)).then(60)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 1)).then(61)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 1)).then(62)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 1)).then(63)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 1)).then(64)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 1)).then(65)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 1)).then(66)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 3) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 1)).then(67)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 3) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 1)).then(68)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 3) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 1)).then(69)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 3) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 1)).then(70)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 2)).then(71)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 2)).then(72)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 2)).then(73)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 2)).then(74)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 4) & (pl.col("VSGR") == 2)).then(75)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 5) & (pl.col("VSGR") == 2)).then(76)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 2)).then(77)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 7) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 1)).then(78)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 7) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 2)).then(79)
        .when(
            (pl.col("RCEG") == 2) & (pl.col("BHBR") == 8) & (pl.col("QLGR") == 0) & (pl.col("VSGR").is_in([1, 2]))).then(
            80)
        .when(
            (pl.col("RCEG") == 2) & (pl.col("BHBR") == 8) & (pl.col("QLGR") == 9) & (pl.col("VSGR").is_in([1, 2]))).then(
            81)
        .when(
            (pl.col("RCEG") == 2) & (pl.col("BHBR") == 0) & (pl.col("QLGR") == 9) & (pl.col("VSGR").is_in([5, 6]))).then(
            82)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 5)).then(83)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 5)).then(84)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 5)).then(85)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 5)).then(86)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 1) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 5)).then(87)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 5)).then(88)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 5)).then(89)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 5)).then(90)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 2) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 5)).then(91)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 3) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 5)).then(92)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 6)).then(93)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 6)).then(94)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 6)).then(95)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 6)).then(96)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 4) & (pl.col("VSGR") == 6)).then(97)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 4) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 6)).then(98)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 6)).then(99)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 6)).then(100)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 6)).then(101)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 6)).then(102)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 4) & (pl.col("VSGR") == 6)).then(103)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 5) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 6)).then(104)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 0) & (pl.col("VSGR") == 6)).then(105)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 1) & (pl.col("VSGR") == 6)).then(106)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 2) & (pl.col("VSGR") == 6)).then(107)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 3) & (pl.col("VSGR") == 6)).then(108)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 4) & (pl.col("VSGR") == 6)).then(109)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 5) & (pl.col("VSGR") == 6)).then(110)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 6) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 6)).then(111)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 7) & (pl.col("QLGR") == 9) & (pl.col("VSGR") == 6)).then(112)
        .when((pl.col("RCEG") == 2) & (pl.col("BHBR") == 79) & (pl.col("QLGR") == 0) & (
            pl.col("VSGR").is_between(1, 4))).then(113)
        .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == 50) & (pl.col("QLGR") == 9) & (
            pl.col("VSGR").is_in([1, 2]))).then(414)
        .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == 79) & (pl.col("QLGR") == 9) & (
            pl.col("VSGR").is_in([1, 2]))).then(415)
        .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == 90) & (pl.col("VSGR").is_in([1, 2]))).then(416)
        .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == 95) & (pl.col("QLGR") == 9) & (
            pl.col("VSGR").is_in([1, 2]))).then(417)
        .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == 99) & (pl.col("QLGR") == 9) & (
            pl.col("VSGR").is_in([1, 2]))).then(418)
        .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == 50) & (pl.col("QLGR").is_between(1, 4))).then(580)
        .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == 77) & (pl.col("QLGR") == 7)).then(581)
        .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == 79) & (pl.col("QLGR") == 0)).then(582)
        .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == 90) & (pl.col("QLGR") == 0)).then(583)
        .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == 95) & (pl.col("QLGR") == 0)).then(584)
        .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == 99) & (pl.col("QLGR") == 7)).then(585)
        .otherwise(None)
        .alias("KOMBI")
    )
    for zaehler in range(25):
        df = df.with_columns(
            pl.when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 0) & (
                        pl.col("VSGR") == 1)).then(114 + zaehler * 5 + 0)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 1) & (
                        pl.col("VSGR") == 1)).then(114 + zaehler * 5 + 1)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 2) & (
                        pl.col("VSGR") == 1)).then(114 + zaehler * 5 + 2)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 3) & (
                        pl.col("VSGR") == 1)).then(114 + zaehler * 5 + 3)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 9) & (
                        pl.col("VSGR") == 1)).then(114 + zaehler * 5 + 4)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 0) & (
                        pl.col("VSGR") == 2)).then(239 + zaehler * 7 + 0)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 1) & (
                        pl.col("VSGR") == 2)).then(239 + zaehler * 7 + 1)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 2) & (
                        pl.col("VSGR") == 2)).then(239 + zaehler * 7 + 2)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 3) & (
                        pl.col("VSGR") == 2)).then(239 + zaehler * 7 + 3)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 4) & (
                        pl.col("VSGR") == 2)).then(239 + zaehler * 7 + 4)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 5) & (
                        pl.col("VSGR") == 2)).then(239 + zaehler * 7 + 5)
            .when((pl.col("RCEG") == 4) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 9) & (
                        pl.col("VSGR") == 2)).then(239 + zaehler * 7 + 6)
            .otherwise(pl.col("KOMBI"))
            .alias("KOMBI")
        )
    for zaehler in range(1, 24):
        base = 419 + (zaehler - 1) * 7
        df = df.with_columns(
            pl.when((pl.col("RCEG") == 5) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 0)).then(base + 0)
            .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 1)).then(base + 1)
            .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 2)).then(base + 2)
            .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 3)).then(base + 3)
            .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 4)).then(base + 4)
            .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 5)).then(base + 5)
            .when((pl.col("RCEG") == 5) & (pl.col("BHBR") == zaehler) & (pl.col("QLGR") == 7)).then(base + 6)
            .otherwise(pl.col("KOMBI"))
            .alias("KOMBI")
        )

    # generate Ses_frg variable
    df = df.with_columns(
        pl.when(pl.col("QLGR").is_in([1, 2, 3, 4, 5, 6, 8, 9])).then(3.0)
        .when(pl.col("QLGR").is_in([0, 7])).then(1.0)
        .otherwise(None)
        .alias("Ses_frg")
    )
    df = df.with_columns(
        pl.when(pl.col("KOMBI") == 0).then(0.0)
        .when(pl.col("KOMBI").is_in([582, 585, 418, 415, 112, 92, 82, 79, 78, 57, 36, 26, 23, 22, 1])).then(4.0)
        .otherwise(pl.col("Ses_frg"))
        .alias("Ses_frg")
    )

    # generate Beruf_frg variable
    beruf_dict = {
        99: [0],
        0: [2, 7, 11, 15, 24, 27, 32, 37, 43, 49, 58, 63, 67, 71, 80, 83, 88, 93, 99, 105, 113],
        1: [8, 9, 10, 11, 12, 13, 14, 64, 65, 66, 68, 69, 70, 120, 121, 122, 123,
            247, 248, 249, 250, 251, 252, 510, 511, 512, 513, 514, 515, 566, 567,
            568, 569, 570, 571],
        2: [4, 5, 6, 29, 30, 31, 33, 34, 35, 41, 42, 60, 61, 62, 85, 86, 87, 89, 90, 91,
            97, 98, 126, 127, 128, 131, 132, 133, 136, 137, 138, 141, 142, 143, 146,
            147, 148, 151, 152, 153, 156, 157, 158, 161, 162, 163, 166, 167, 168, 171,
            172, 173, 176, 177, 178, 181, 182, 183, 258, 265, 272, 279, 286, 293, 294,
            300, 259, 266, 273, 280, 287, 301, 307, 314, 321, 328, 335, 308, 315, 322,
            329, 336, 419, 424, 431, 438, 445, 452, 459, 466, 473, 480, 487, 494, 501,
            508, 578, 573, 426, 433, 440, 447, 454, 461, 468, 475, 482, 489, 496, 503],
        3: [3, 28, 40, 45, 46, 47, 48, 59, 84, 96, 101, 102, 103, 104, 125, 130, 135,
            140, 145, 150, 155, 160, 165, 170, 175, 180, 256, 263, 270, 277, 284,
            291, 292, 298, 305, 312, 319, 326, 333, 422, 429, 436, 443, 450, 457,
            464, 471, 478, 485, 492, 499, 506, 576, 257, 264, 271, 278, 285, 299,
            306, 313, 320, 327, 334, 423, 430, 437, 444, 451, 458, 465, 472, 479,
            486, 493, 500, 507, 577],
        4: [38, 39, 44, 94, 95, 100, 255, 262, 269, 276, 283, 290, 297, 304, 311, 318,
            325, 332, 421, 428, 435, 442, 449, 456, 463, 470, 477, 484, 491, 498, 505,
            575],
        5: [254, 261, 268, 275, 282, 289, 296, 303, 310, 317, 324, 331, 420, 427,
            434, 441, 448, 455, 462, 469, 476, 483, 490, 497, 504, 574],
        6: [19, 20, 21, 25, 75, 76, 77, 81, 186, 187, 188, 191, 192, 193, 196, 197, 198,
            201, 202, 203, 206, 207, 208, 211, 212, 213, 216, 217, 218, 221, 222, 223,
            236, 237, 238, 342, 343, 349, 350, 356, 357, 363, 364, 370, 371, 377, 378,
            384, 385, 391, 392, 412, 413, 517, 522, 524, 529, 531, 536, 538, 543, 545,
            550, 559, 564],
        7: [18, 74, 185, 190, 195, 200, 205, 210, 215, 220, 235, 340, 341, 347, 348,
            354, 355, 361, 362, 368, 369, 375, 376, 382, 383, 389, 390, 410, 411,
            520, 521, 527, 528, 534, 535, 541, 542, 548, 549, 562, 563],
        8: [17, 73, 339, 346, 353, 360, 367, 374, 381, 388, 409, 519, 526, 533, 540, 547, 561],
        9: [16, 72, 338, 345, 352, 359, 366, 373, 380, 387, 408, 518, 525, 532, 539, 546, 560],
        10: [53, 54, 55, 109, 110, 111, 226, 227, 228, 231, 232, 233, 397, 398, 399, 404, 405, 406, 552, 556, 557],
        11: [51, 52, 107, 108, 225, 230, 395, 396, 402, 403, 554, 555],
        12: [553, 401, 394, 106, 50],
        13: [1, 22, 23, 26, 36, 56, 57, 78, 79, 82, 92, 112, 115, 116, 117, 118,
             240, 241, 242, 243, 244, 245, 414, 415, 416, 583, 580, 582, 585],
    }
    # invert beruf_dict & apply as map to KOMBI
    beruf_map = {val: key for key, val_list in beruf_dict.items()
                 for val in val_list}

    df = df.with_columns(
        pl.when(pl.col("KOMBI").is_in(list(beruf_map.keys())))
        .then(pl.col("KOMBI").replace(beruf_map))
        .otherwise(None)
        .alias("Beruf_frg")
    )

    df = df.with_columns(
        pl.when((pl.col("Beruf_frg").is_null()) & (pl.col("Ses_frg").is_not_null()))
        .then(13)
        .otherwise(pl.col("Beruf_frg"))
        .alias("Beruf_frg")
    )

    # adjust for Wehr- und Zivildienst
    df = df.with_columns(
        pl.when((pl.col('Beruf_frg') == 0) & (pl.col('SES') == 9))
        .then(14)
        .otherwise(pl.col("Beruf_frg"))
        .alias("Beruf_frg")
    )

    # "Umcodierung aufgrund unplausibler Werte"
    df = df.with_columns(
        pl.when((pl.col('Ses_frg') == 1) & (pl.col('SES') == 9))
        .then(2)
        .otherwise(pl.col("Ses_frg"))
        .alias('Ses_frg')
    )
    kombi_list = [
        3, 4, 5, 12, 20, 59, 60, 61, 64, 76, 85, 90,
        421, 423, 424, 428, 430, 431, 435, 437, 438, 444, 445, 451,
        452, 455, 456, 458, 459, 562, 465, 466, 472, 473, 478, 479,
        480, 484, 486, 487, 490, 491, 493, 494, 497, 498, 500, 501,
        507, 508, 512, 513, 514, 515, 519, 521, 522, 528, 529, 532,
        533, 535, 536, 539, 540, 542, 543, 546, 547, 549, 550, 554,
        556, 557, 563, 564, 570, 571, 577, 578
    ]
    df = df.with_columns(
        pl.when(pl.col("KOMBI").is_in(kombi_list))
        .then(3)
        .otherwise(pl.col("Ses_frg"))
        .alias("Ses_frg")
    )
    df = df.drop("KOMBI")

    return df

#####################################################################################################################
