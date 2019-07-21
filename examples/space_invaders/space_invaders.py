import curses
import logging
import random
import re
import sys
import textwrap
import time

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


_PY3 = sys.version_info > (3, 0)
if _PY3:
    xrange = range


logging.basicConfig(
    filename="space_invaders.log",
    format="%(asctime)s,%(msecs)03d %(levelname)-5.5s %(message)s",
)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

Base = declarative_base()

WINDOW_LEFT = 10
WINDOW_TOP = 2
WINDOW_WIDTH = 70
WINDOW_HEIGHT = 34
VERT_PADDING = 2
HORIZ_PADDING = 5
ENEMY_VERT_SPACING = 4
MAX_X = WINDOW_WIDTH - HORIZ_PADDING
MAX_Y = WINDOW_HEIGHT - VERT_PADDING
LEFT_KEY = ord("j")
RIGHT_KEY = ord("l")
FIRE_KEY = ord(" ")
PAUSE_KEY = ord("p")

COLOR_MAP = {
    "K": curses.COLOR_BLACK,
    "B": curses.COLOR_BLUE,
    "C": curses.COLOR_CYAN,
    "G": curses.COLOR_GREEN,
    "M": curses.COLOR_MAGENTA,
    "R": curses.COLOR_RED,
    "W": curses.COLOR_WHITE,
    "Y": curses.COLOR_YELLOW,
}


class Glyph(Base):
    """Describe a "glyph", a graphical element
    to be painted on the screen.

    """

    __tablename__ = "glyph"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)
    width = Column(Integer)
    height = Column(Integer)
    data = Column(String)
    alt_data = Column(String)
    __mapper_args__ = {"polymorphic_on": type}

    def __init__(self, name, img, alt=None):
        self.name = name
        self.data, self.width, self.height = self._encode_glyph(img)
        if alt is not None:
            self.alt_data, alt_w, alt_h = self._encode_glyph(alt)

    def _encode_glyph(self, img):
        """Receive a textual description of the glyph and
        encode into a format understood by
        GlyphCoordinate.render().

        """
        img = re.sub(r"^\n", "", textwrap.dedent(img))
        color = "W"
        lines = [line.rstrip() for line in img.split("\n")]
        data = []
        for line in lines:
            render_line = []
            line = list(line)
            while line:
                char = line.pop(0)
                if char == "#":
                    color = line.pop(0)
                    continue
                render_line.append((color, char))
            data.append(render_line)
        width = max([len(rl) for rl in data])
        data = "".join(
            "".join("%s%s" % (color, char) for color, char in render_line)
            + ("W " * (width - len(render_line)))
            for render_line in data
        )
        return data, width, len(lines)

    def glyph_for_state(self, coord, state):
        """Return the appropriate data representation
        for this Glyph, based on the current coordinates
        and state.

        Subclasses may override this to provide animations.

        """
        return self.data


class GlyphCoordinate(Base):
    """Describe a glyph rendered at a certain x, y coordinate.

    The GlyphCoordinate may also include optional values
    such as the tick at time of render, a label, and a
    score value.

    """

    __tablename__ = "glyph_coordinate"
    id = Column(Integer, primary_key=True)
    glyph_id = Column(Integer, ForeignKey("glyph.id"))
    x = Column(Integer)
    y = Column(Integer)
    tick = Column(Integer)
    label = Column(String)
    score = Column(Integer)
    glyph = relationship(Glyph, innerjoin=True)

    def __init__(
        self, session, glyph_name, x, y, tick=None, label=None, score=None
    ):
        self.glyph = session.query(Glyph).filter_by(name=glyph_name).one()
        self.x = x
        self.y = y
        self.tick = tick
        self.label = label
        self.score = score
        session.add(self)

    def render(self, window, state):
        """Render the Glyph at this position."""

        col = 0
        row = 0
        glyph = self.glyph
        data = glyph.glyph_for_state(self, state)
        for color, char in [
            (data[i], data[i + 1]) for i in xrange(0, len(data), 2)
        ]:

            x = self.x + col
            y = self.y + row
            if 0 <= x <= MAX_X and 0 <= y <= MAX_Y:
                window.addstr(
                    y + VERT_PADDING,
                    x + HORIZ_PADDING,
                    char,
                    _COLOR_PAIRS[color],
                )
            col += 1
            if col == glyph.width:
                col = 0
                row += 1
        if self.label:
            self._render_label(window, False)

    def _render_label(self, window, blank):
        label = self.label if not blank else " " * len(self.label)
        if self.x + self.width + len(self.label) < MAX_X:
            window.addstr(self.y, self.x + self.width, label)
        else:
            window.addstr(self.y, self.x - len(self.label), label)

    def blank(self, window):
        """Render a blank box for this glyph's position and size."""

        glyph = self.glyph
        x = min(max(self.x, 0), MAX_X)
        width = min(glyph.width, MAX_X - x) or 1
        for y_a in xrange(self.y, self.y + glyph.height):
            y = y_a
            window.addstr(y + VERT_PADDING, x + HORIZ_PADDING, " " * width)

        if self.label:
            self._render_label(window, True)

    @hybrid_property
    def width(self):
        return self.glyph.width

    @width.expression
    def width(cls):
        return Glyph.width

    @hybrid_property
    def height(self):
        return self.glyph.height

    @height.expression
    def height(cls):
        return Glyph.height

    @hybrid_property
    def bottom_bound(self):
        return self.y + self.height >= MAX_Y

    @hybrid_property
    def top_bound(self):
        return self.y <= 0

    @hybrid_property
    def left_bound(self):
        return self.x <= 0

    @hybrid_property
    def right_bound(self):
        return self.x + self.width >= MAX_X

    @hybrid_property
    def right_edge_bound(self):
        return self.x > MAX_X

    @hybrid_method
    def intersects(self, other):
        """Return True if this GlyphCoordinate intersects with
        the given GlyphCoordinate."""

        return ~(
            (self.x + self.width < other.x) | (self.x > other.x + other.width)
        ) & ~(
            (self.y + self.height < other.y)
            | (self.y > other.y + other.height)
        )


class EnemyGlyph(Glyph):
    """Describe an enemy."""

    __mapper_args__ = {"polymorphic_identity": "enemy"}


class ArmyGlyph(EnemyGlyph):
    """Describe an enemy that's part of the "army". """

    __mapper_args__ = {"polymorphic_identity": "army"}

    def glyph_for_state(self, coord, state):
        if state["flip"]:
            return self.alt_data
        else:
            return self.data


class SaucerGlyph(EnemyGlyph):
    """Describe the enemy saucer flying overhead."""

    __mapper_args__ = {"polymorphic_identity": "saucer"}

    def glyph_for_state(self, coord, state):
        if state["flip"] == 0:
            return self.alt_data
        else:
            return self.data


class MessageGlyph(Glyph):
    """Describe a glyph for displaying a message."""

    __mapper_args__ = {"polymorphic_identity": "message"}


class PlayerGlyph(Glyph):
    """Describe a glyph representing the player."""

    __mapper_args__ = {"polymorphic_identity": "player"}


class MissileGlyph(Glyph):
    """Describe a glyph representing a missile."""

    __mapper_args__ = {"polymorphic_identity": "missile"}


class SplatGlyph(Glyph):
    """Describe a glyph representing a "splat"."""

    __mapper_args__ = {"polymorphic_identity": "splat"}

    def glyph_for_state(self, coord, state):
        age = state["tick"] - coord.tick
        if age > 5:
            return self.alt_data
        else:
            return self.data


def init_glyph(session):
    """Create the glyphs used during play."""

    enemy1 = ArmyGlyph(
        "enemy1",
        """
         #W-#B^#R-#B^#W-
         #G|   |
        """,
        """
         #W>#B^#R-#B^#W<
         #G^   ^
        """,
    )

    enemy2 = ArmyGlyph(
        "enemy2",
        """
         #W***
        #R<#C~~~#R>
        """,
        """
         #W@@@
        #R<#C---#R>
        """,
    )

    enemy3 = ArmyGlyph(
        "enemy3",
        """
        #Y((--))
        #M-~-~-~
        """,
        """
        #Y[[--]]
        #M~-~-~-
        """,
    )

    saucer = SaucerGlyph(
        "saucer",
        """#R~#Y^#R~#G<<((=#WOO#G=))>>""",
        """#Y^#R~#Y^#G<<((=#WOO#G=))>>""",
    )

    splat1 = SplatGlyph(
        "splat1",
        """
             #WVVVVV
            #W> #R*** #W<
             #W^^^^^
        """,
        """
                #M|
             #M- #Y+++ #M-
                #M|
        """,
    )

    ship = PlayerGlyph(
        "ship",
        """
       #Y^
     #G=====
    """,
    )

    missile = MissileGlyph(
        "missile",
        """
        |
    """,
    )

    start = MessageGlyph(
        "start_message",
        "J = move left; L = move right; SPACE = fire\n"
        "           #GPress any key to start",
    )
    lose = MessageGlyph("lose_message", "#YY O U  L O S E ! ! !")
    win = MessageGlyph("win_message", "#RL E V E L  C L E A R E D ! ! !")
    paused = MessageGlyph(
        "pause_message", "#WP A U S E D\n#GPress P to continue"
    )
    session.add_all(
        [
            enemy1,
            enemy2,
            enemy3,
            ship,
            saucer,
            missile,
            start,
            lose,
            win,
            paused,
            splat1,
        ]
    )


def setup_curses():
    """Setup terminal/curses state."""

    window = curses.initscr()
    curses.noecho()

    window = curses.newwin(
        WINDOW_HEIGHT + (VERT_PADDING * 2),
        WINDOW_WIDTH + (HORIZ_PADDING * 2),
        WINDOW_TOP - VERT_PADDING,
        WINDOW_LEFT - HORIZ_PADDING,
    )
    curses.start_color()

    global _COLOR_PAIRS
    _COLOR_PAIRS = {}
    for i, (k, v) in enumerate(COLOR_MAP.items(), 1):
        curses.init_pair(i, v, curses.COLOR_BLACK)
        _COLOR_PAIRS[k] = curses.color_pair(i)
    return window


def init_positions(session):
    """Establish a new field of play.

    This generates GlyphCoordinate objects
    and persists them to the database.

    """

    # delete all existing coordinates
    session.query(GlyphCoordinate).delete()

    session.add(
        GlyphCoordinate(
            session, "ship", WINDOW_WIDTH // 2 - 2, WINDOW_HEIGHT - 4
        )
    )

    arrangement = (
        ("enemy3", 50),
        ("enemy2", 25),
        ("enemy1", 10),
        ("enemy2", 25),
        ("enemy1", 10),
    )
    for (ship_vert, (etype, score)) in zip(
        xrange(5, 30, ENEMY_VERT_SPACING), arrangement
    ):
        for ship_horiz in xrange(0, 50, 10):
            session.add(
                GlyphCoordinate(
                    session, etype, ship_horiz, ship_vert, score=score
                )
            )


def draw(session, window, state):
    """Load all current GlyphCoordinate objects from the
    database and render.

    """
    for gcoord in session.query(GlyphCoordinate).options(joinedload("glyph")):
        gcoord.render(window, state)
    window.addstr(1, WINDOW_WIDTH - 5, "Score: %.4d" % state["score"])
    window.move(0, 0)
    window.refresh()


def check_win(session, state):
    """Return the number of army glyphs remaining -
    the player wins if this is zero."""

    return (
        session.query(func.count(GlyphCoordinate.id))
        .join(GlyphCoordinate.glyph.of_type(ArmyGlyph))
        .scalar()
    )


def check_lose(session, state):
    """Return the number of army glyphs either colliding
    with the player or hitting the bottom of the screen.

    The player loses if this is non-zero."""

    player = state["player"]
    return (
        session.query(GlyphCoordinate)
        .join(GlyphCoordinate.glyph.of_type(ArmyGlyph))
        .filter(
            GlyphCoordinate.intersects(player) | GlyphCoordinate.bottom_bound
        )
        .count()
    )


def render_message(session, window, msg, x, y):
    """Render a message glyph.

    Clears the area beneath the message first
    and assumes the display will be paused
    afterwards.

    """
    # create message box
    msg = GlyphCoordinate(session, msg, x, y)

    # clear existing glyphs which intersect
    for gly in (
        session.query(GlyphCoordinate)
        .join(GlyphCoordinate.glyph)
        .filter(GlyphCoordinate.intersects(msg))
    ):
        gly.blank(window)

    # render
    msg.render(window, {})
    window.refresh()
    return msg


def win(session, window, state):
    """Handle the win case."""
    render_message(session, window, "win_message", 15, 15)
    time.sleep(2)
    start(session, window, state, True)


def lose(session, window, state):
    """Handle the lose case."""
    render_message(session, window, "lose_message", 15, 15)
    time.sleep(2)
    start(session, window, state)


def pause(session, window, state):
    """Pause the game."""
    msg = render_message(session, window, "pause_message", 15, 15)
    prompt(window)
    msg.blank(window)
    session.delete(msg)


def prompt(window):
    """Display a prompt, quashing any keystrokes
    which might have remained."""

    window.move(0, 0)
    window.nodelay(1)
    window.getch()
    window.nodelay(0)
    window.getch()
    window.nodelay(1)


def move_army(session, window, state):
    """Update the army position based on the current
    size of the field."""
    speed = 30 // 25 * state["num_enemies"]

    flip = (state["tick"] % speed) == 0

    if not flip:
        return
    else:
        state["flip"] = not state["flip"]

    x_slide = 1

    # get the lower/upper boundaries of the army
    # along the X axis.
    min_x, max_x = (
        session.query(
            func.min(GlyphCoordinate.x),
            func.max(GlyphCoordinate.x + GlyphCoordinate.width),
        )
        .join(GlyphCoordinate.glyph.of_type(ArmyGlyph))
        .first()
    )

    if min_x is None or max_x is None:
        # no enemies
        return

    direction = state["army_direction"]
    move_y = False
    if direction == 0 and max_x + x_slide >= MAX_X:
        direction = state["army_direction"] = 1
        move_y = True
    elif direction == 1 and min_x - x_slide <= 0:
        direction = state["army_direction"] = 0
        move_y = True

    for enemy_g in session.query(GlyphCoordinate).join(
        GlyphCoordinate.glyph.of_type(ArmyGlyph)
    ):
        enemy_g.blank(window)

        if move_y:
            enemy_g.y += 1
        elif direction == 0:
            enemy_g.x += x_slide
        elif direction == 1:
            enemy_g.x -= x_slide


def move_player(session, window, state):
    """Receive player input and adjust state."""

    ch = window.getch()
    if ch not in (LEFT_KEY, RIGHT_KEY, FIRE_KEY, PAUSE_KEY):
        return
    elif ch == PAUSE_KEY:
        pause(session, window, state)
        return

    player = state["player"]
    if ch == RIGHT_KEY and not player.right_bound:
        player.blank(window)
        player.x += 1
    elif ch == LEFT_KEY and not player.left_bound:
        player.blank(window)
        player.x -= 1
    elif ch == FIRE_KEY and state["missile"] is None:
        state["missile"] = GlyphCoordinate(
            session, "missile", player.x + 3, player.y - 1
        )


def move_missile(session, window, state):
    """Update the status of the current missile, if any."""

    if state["missile"] is None or state["tick"] % 2 != 0:
        return

    missile = state["missile"]

    # locate enemy glyphs which intersect with the
    # missile's current position; i.e. a hit
    glyph = (
        session.query(GlyphCoordinate)
        .join(GlyphCoordinate.glyph.of_type(EnemyGlyph))
        .filter(GlyphCoordinate.intersects(missile))
        .first()
    )
    missile.blank(window)
    if glyph or missile.top_bound:
        # missile is done
        session.delete(missile)
        state["missile"] = None
        if glyph:
            # score!
            score(session, window, state, glyph)
    else:
        # move missile up one character.
        missile.y -= 1


def move_saucer(session, window, state):
    """Update the status of the saucer."""

    saucer_interval = 500
    saucer_speed_interval = 4
    if state["saucer"] is None and state["tick"] % saucer_interval != 0:
        return

    if state["saucer"] is None:
        state["saucer"] = saucer = GlyphCoordinate(
            session, "saucer", -6, 1, score=random.randrange(100, 600, 100)
        )
    elif state["tick"] % saucer_speed_interval == 0:
        saucer = state["saucer"]
        saucer.blank(window)
        saucer.x += 1
        if saucer.right_edge_bound:
            session.delete(saucer)
            state["saucer"] = None


def update_splat(session, window, state):
    """Render splat animations."""

    for splat in session.query(GlyphCoordinate).join(
        GlyphCoordinate.glyph.of_type(SplatGlyph)
    ):
        age = state["tick"] - splat.tick
        if age > 10:
            splat.blank(window)
            session.delete(splat)
        else:
            splat.render(window, state)


def score(session, window, state, glyph):
    """Process a glyph intersecting with a missile."""

    glyph.blank(window)
    session.delete(glyph)
    if state["saucer"] is glyph:
        state["saucer"] = None
    state["score"] += glyph.score
    # render a splat !
    GlyphCoordinate(
        session,
        "splat1",
        glyph.x,
        glyph.y,
        tick=state["tick"],
        label=str(glyph.score),
    )


def update_state(session, window, state):
    """Update all state for each game tick."""

    num_enemies = state["num_enemies"] = check_win(session, state)
    if num_enemies == 0:
        win(session, window, state)
    elif check_lose(session, state):
        lose(session, window, state)
    else:
        # update the tick counter.
        state["tick"] += 1
        move_player(session, window, state)
        move_missile(session, window, state)
        move_army(session, window, state)
        move_saucer(session, window, state)
        update_splat(session, window, state)


def start(session, window, state, continue_=False):
    """Start a new field of play."""

    render_message(session, window, "start_message", 15, 20)
    prompt(window)

    init_positions(session)

    player = (
        session.query(GlyphCoordinate)
        .join(GlyphCoordinate.glyph.of_type(PlayerGlyph))
        .one()
    )
    state.update(
        {
            "field_pos": 0,
            "alt": False,
            "tick": 0,
            "missile": None,
            "saucer": None,
            "player": player,
            "army_direction": 0,
            "flip": False,
        }
    )
    if not continue_:
        state["score"] = 0

    window.clear()
    window.box()
    draw(session, window, state)


def main():
    """Initialize the database and establish the game loop."""

    e = create_engine("sqlite://")
    Base.metadata.create_all(e)
    session = Session(e)
    init_glyph(session)
    session.commit()
    window = setup_curses()
    state = {}
    start(session, window, state)
    while True:
        update_state(session, window, state)
        draw(session, window, state)
        time.sleep(0.01)


if __name__ == "__main__":
    main()
