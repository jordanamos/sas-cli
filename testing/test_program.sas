%LET message = Hello silly SAS user, this is just a test;
%PUT &message.;

DATA _hi;
    SET=C_JA.hi;
RUN;